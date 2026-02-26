# CEI-InOE Database Schema

## Overview

The CEI-InOE data warehouse uses PostgreSQL with a layered architecture:
- **Fact Tables**: Final analytical data (energy, environmental, dairy)
- **Staging Tables**: Intermediate validation/transformation layer
- **Metadata Tables**: Pipeline execution, quality tracking, device registry
- **Views**: Pre-aggregated analytical views

## Entity Relationship Diagram

```
                                    ┌──────────────────────┐
                                    │    generic_device    │
                                    │ (unified device reg) │
                                    └──────────┬───────────┘
                                               │
              ┌────────────────────────────────┼─────────────────────────────────┐
              │                                │                                 │
              ▼                                ▼                                 ▼
┌─────────────────────────┐    ┌─────────────────────────┐    ┌─────────────────────────┐
│   fact_energy_hourly    │    │   fact_energy_daily     │    │  environmental_metrics  │
│   device_id → device    │    │   device_id → device    │    │                         │
│   source_file → file    │    │   source_file → file    │    │   source_file → file    │
└─────────────────────────┘    └─────────────────────────┘    └─────────────────────────┘
              ▲                                ▲                                 ▲
              │                                │                                 │
              └────────────────────────────────┼─────────────────────────────────┘
                                               │
                                    ┌──────────┴───────────┐
                                    │     ingest_file      │
                                    │  (file registration  │
                                    │   + deduplication)   │
                                    └──────────┬───────────┘
                                               │
                ┌──────────────────────────────┼───────────────────────────────┐
                │                              │                               │
                ▼                              ▼                               ▼
┌───────────────────────────┐  ┌───────────────────────────┐  ┌───────────────────────────┐
│  staging_energy_hourly    │  │  staging_energy_daily     │  │staging_environmental_metr.│
│  (validation + transform) │  │  (validation + transform) │  │  (validation + transform) │
└───────────────────────────┘  └───────────────────────────┘  └───────────────────────────┘

                                    ┌─────────────────────┐
                                    │    ingest_file      │
                                    └──────────┬──────────┘
                                               │
                                               ▼
                   ┌───────────────────────────┼───────────────────────────┐
                   │                           │                           │
                   ▼                           ▼                           ▼
     ┌─────────────────────┐    ┌─────────────────────────┐    ┌─────────────────────────┐
     │  pipeline_execution │    │   data_quality_checks   │    │    api_fetch_cursor     │
     │  (stage tracking)   │    │   (validation results)  │    │   (incremental fetch)   │
     └─────────────────────┘    └─────────────────────────┘    └─────────────────────────┘
```

---

## Core Tables

### generic_device

Unified device registry supporting multiple device types (energy, weather, dairy, etc.).

```sql
CREATE TABLE generic_device (
    id              SERIAL PRIMARY KEY,
    device_id       VARCHAR(64) NOT NULL UNIQUE,  -- External identifier
    device_type     VARCHAR(32) NOT NULL,         -- 'energy', 'weather', 'dairy'
    alias           VARCHAR(255),                 -- Human-readable name
    client          VARCHAR(255) NOT NULL,        -- Owner/customer
    description     TEXT,
    status          VARCHAR(32) DEFAULT 'unknown', -- 'online', 'offline', 'unknown'
    timezone        VARCHAR(64) DEFAULT 'UTC',
    metadata        JSONB NOT NULL DEFAULT '{}',  -- Type-specific data
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
```

**Metadata Examples:**
- Energy devices: `{"device_token": "abc123", "name": "Farm A Meter"}`
- Weather devices: `{"external_id": "airbeld_123", "location": "Field B"}`

### ingest_file

File registration and deduplication tracking.

```sql
CREATE TABLE ingest_file (
    file_id           UUID PRIMARY KEY,
    file_name         TEXT NOT NULL,
    device_id         TEXT NOT NULL,
    granularity       TEXT NOT NULL,              -- 'hourly', 'daily'
    start_date        DATE,
    end_date          DATE,
    sha256            TEXT UNIQUE NOT NULL,       -- Deduplication key
    ingested_at       TIMESTAMPTZ DEFAULT NOW(),
    execution_time_ms INT,                        -- Pipeline duration
    validation_status TEXT,                       -- 'passed', 'failed', 'partial'
    quality_score     NUMERIC(5,2),               -- Percentage valid
    pipeline_version  TEXT DEFAULT '1.0'
);
```

---

## Fact Tables

### fact_energy_hourly

Hourly energy consumption readings per device.

```sql
CREATE TABLE fact_energy_hourly (
    energy_id         SERIAL PRIMARY KEY,
    device_id         INTEGER NOT NULL REFERENCES generic_device(id),
    ts                TIMESTAMP NOT NULL,         -- Measurement timestamp
    energy_kwh        FLOAT NOT NULL,             -- kWh consumed
    source_file       UUID REFERENCES ingest_file(file_id),
    source_type       TEXT DEFAULT 'csv',         -- 'csv', 'api', 'excel'
    source_api_endpoint TEXT,                     -- API URL if applicable
    source_device_id  TEXT,                       -- Original device ID string
    ingestion_method  TEXT DEFAULT 'batch',       -- 'batch', 'streaming'
    ingested_at       TIMESTAMPTZ DEFAULT NOW(),
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(device_id, ts)                         -- One reading per device per hour
);
```

### fact_energy_daily

Daily aggregated energy consumption.

```sql
CREATE TABLE fact_energy_daily (
    energy_id         SERIAL PRIMARY KEY,
    device_id         INTEGER NOT NULL REFERENCES generic_device(id),
    ts                DATE NOT NULL,              -- Measurement date
    energy_kwh        FLOAT NOT NULL,             -- Daily kWh consumed
    source_file       UUID REFERENCES ingest_file(file_id),
    source_type       TEXT DEFAULT 'csv',
    source_api_endpoint TEXT,
    source_device_id  TEXT,
    ingestion_method  TEXT DEFAULT 'batch',
    ingested_at       TIMESTAMPTZ DEFAULT NOW(),
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(device_id, ts)                         -- One reading per device per day
);
```

### environmental_metrics

Environmental sensor readings (weather station data).

```sql
CREATE TABLE environmental_metrics (
    id                     SERIAL PRIMARY KEY,
    timestamp              TIMESTAMPTZ NOT NULL UNIQUE,
    atm_pressure           DECIMAL(8, 2),         -- Atmospheric pressure (hPa)
    noise_level_db         DECIMAL(6, 2),         -- Noise level (dB)
    temperature            DECIMAL(6, 2),         -- Temperature (°C)
    humidity               DECIMAL(6, 2),         -- Relative humidity (%)
    pm10                   DECIMAL(8, 2),         -- PM10 particulate (µg/m³)
    wind_speed             DECIMAL(6, 2),         -- Wind speed (m/s or km/h)
    wind_direction_sectors DECIMAL(6, 2),         -- Wind direction sectors
    wind_angle             DECIMAL(6, 2),         -- Wind angle (degrees)
    pm2p5                  DECIMAL(8, 2),         -- PM2.5 particulate (µg/m³)
    source_file            UUID REFERENCES ingest_file(file_id),
    source_type            TEXT DEFAULT 'csv',
    source_api_endpoint    TEXT,
    source_device_id       TEXT,
    ingestion_method       TEXT DEFAULT 'batch',
    ingested_at            TIMESTAMPTZ DEFAULT NOW(),
    created_at             TIMESTAMPTZ DEFAULT NOW()
);
```

### dairy_production

Daily dairy farm production metrics.

```sql
CREATE TABLE dairy_production (
    id                       SERIAL PRIMARY KEY,
    production_date          DATE NOT NULL UNIQUE,
    day_production_per_cow_kg DECIMAL(10, 2),      -- Milk per cow (kg)
    number_of_animals        INTEGER,             -- Herd size
    average_lactation_days   INTEGER,             -- Avg lactation period
    fed_per_cow_total_kg     DECIMAL(10, 2),      -- Feed per cow (kg)
    fed_per_cow_water_kg     DECIMAL(10, 2),      -- Water per cow (kg)
    feed_efficiency          DECIMAL(10, 4),      -- Milk/feed ratio
    rumination_minutes       INTEGER,             -- Daily rumination time
    source_file              UUID REFERENCES ingest_file(file_id),
    source_type              TEXT DEFAULT 'csv',
    source_api_endpoint      TEXT,
    source_device_id         TEXT,
    ingestion_method         TEXT DEFAULT 'batch',
    ingested_at              TIMESTAMPTZ DEFAULT NOW()
);
```

---

## Staging Tables

Staging tables mirror fact tables with additional validation/transformation columns.

### staging_energy_hourly

```sql
CREATE TABLE staging_energy_hourly (
    staging_id        SERIAL PRIMARY KEY,
    file_id           UUID REFERENCES ingest_file(file_id),
    row_number        INT NOT NULL,              -- Source row for debugging
    raw_data          JSONB NOT NULL,            -- Original CSV/API record
    transformed_data  JSONB,                     -- Post-transformation data
    validation_errors JSONB,                     -- List of validation errors
    is_valid          BOOLEAN DEFAULT FALSE,
    loaded_to_final   BOOLEAN DEFAULT FALSE,
    created_at        TIMESTAMPTZ DEFAULT NOW()
);
```

**Staging tables exist for:**
- `staging_energy_hourly`
- `staging_energy_daily`
- `staging_environmental_metrics`
- `staging_dairy_production`

**Mapping in Code (StagingDAO):**
```python
STAGING_TABLES = {
    'environmental_metrics': 'staging_environmental_metrics',
    'energy_hourly': 'staging_energy_hourly',
    'energy_daily': 'staging_energy_daily',
    'dairy_production': 'staging_dairy_production'
}
```

---

## Pipeline Metadata Tables

### pipeline_execution

Tracks each stage of pipeline execution for observability.

```sql
CREATE TABLE pipeline_execution (
    id                  SERIAL PRIMARY KEY,
    file_id             UUID REFERENCES ingest_file(file_id),
    pipeline_name       TEXT NOT NULL,           -- e.g., 'energy_hourly_pipeline'
    stage               TEXT NOT NULL,           -- 'extract', 'validate', 'transform', 'load'
    started_at          TIMESTAMPTZ DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    status              TEXT NOT NULL,           -- 'running', 'success', 'failed', 'skipped'
    records_in          INT DEFAULT 0,
    records_out         INT DEFAULT 0,
    error_message       TEXT,
    execution_metadata  JSONB,                   -- Additional context
    created_at          TIMESTAMPTZ DEFAULT NOW()
);
```

### data_quality_checks

Records data quality validation results.

```sql
CREATE TABLE data_quality_checks (
    id              SERIAL PRIMARY KEY,
    file_id         UUID REFERENCES ingest_file(file_id),
    dataset         TEXT NOT NULL,               -- 'energy_hourly', 'environmental_metrics'
    check_type      TEXT NOT NULL,               -- 'schema', 'type', 'range', 'uniqueness'
    check_name      TEXT NOT NULL,               -- 'temperature_range', 'required_fields'
    passed          BOOLEAN NOT NULL,
    failed_count    INT DEFAULT 0,
    total_count     INT DEFAULT 0,
    failure_rate    NUMERIC(5,2),                -- Percentage
    sample_failures JSONB,                       -- Examples for debugging
    checked_at      TIMESTAMPTZ DEFAULT NOW()
);
```

### api_fetch_cursor

Tracks last successful API fetch timestamp for incremental fetching.

```sql
CREATE TABLE api_fetch_cursor (
    id                   SERIAL PRIMARY KEY,
    connector_id         TEXT NOT NULL,          -- 'tago_energy', 'airbeld_environmental'
    endpoint_id          TEXT NOT NULL,          -- 'hourly_consumption', 'diffday'
    device_id            TEXT NOT NULL,          -- Device external ID
    last_fetch_timestamp TIMESTAMPTZ NOT NULL,   -- Last record timestamp fetched
    last_fetch_success   TIMESTAMPTZ DEFAULT NOW(),
    fetch_count          INT DEFAULT 1,
    created_at           TIMESTAMPTZ DEFAULT NOW(),
    updated_at           TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(connector_id, endpoint_id, device_id)
);
```

---

## Pydantic Data Models

Located in `ingestor/app/models.py`, these provide type-safe Python representations.

### BaseRecord

Base class for all dataset records with source tracking metadata.

```python
class BaseRecord(BaseModel):
    source_type: Optional[SourceType] = None        # csv, api, excel
    source_file: Optional[UUID] = None
    source_api_endpoint: Optional[str] = None
    source_device_id: Optional[str] = None
    ingestion_method: Optional[str] = "batch"
    ingested_at: Optional[datetime] = None
```

### EnergyHourlyRecord

```python
class EnergyHourlyRecord(BaseRecord):
    ts: datetime                                    # TIMESTAMP NOT NULL
    energy_kwh: float = Field(ge=0, le=10000)       # FLOAT NOT NULL, 0-10000
    device_id: Optional[int] = None                 # FK injected post-validation

    @field_validator('ts', mode='before')
    def validate_ts(cls, v):
        return parse_datetime_with_ampm(v)          # Handles "MM/DD/YYYY HH:MM:SS AM"

    @field_validator('energy_kwh', mode='before')
    def validate_energy_kwh(cls, v):
        return parse_european_float(v)              # Handles "1.234,56"
```

### EnergyDailyRecord

```python
class EnergyDailyRecord(BaseRecord):
    ts: date                                        # DATE NOT NULL
    energy_kwh: float = Field(ge=0, le=100000)      # Daily max higher
    device_id: Optional[int] = None
```

### EnvironmentalMetricsRecord

```python
class EnvironmentalMetricsRecord(BaseRecord):
    timestamp: datetime
    temperature: float = Field(ge=-50, le=60)       # Celsius range
    atm_pressure: Optional[float] = None
    noise_level_db: Optional[float] = Field(default=None, ge=0, le=140)
    humidity: Optional[float] = Field(default=None, ge=0, le=100)
    pm10: Optional[float] = Field(default=None, ge=0, le=1000)
    wind_speed: Optional[float] = Field(default=None, ge=0, le=150)
    wind_direction_sectors: Optional[float] = None
    wind_angle: Optional[float] = Field(default=None, ge=0, le=360)
    pm2p5: Optional[float] = Field(default=None, ge=0, le=500)
```

### DairyProductionRecord

```python
class DairyProductionRecord(BaseRecord):
    production_date: date
    number_of_animals: int = Field(ge=1, le=10000)
    day_production_per_cow_kg: Optional[float] = Field(default=None, ge=0, le=100)
    average_lactation_days: Optional[int] = Field(default=None, ge=0, le=1000)
    fed_per_cow_total_kg: Optional[float] = Field(default=None, ge=0, le=200)
    fed_per_cow_water_kg: Optional[float] = Field(default=None, ge=0, le=100)
    feed_efficiency: Optional[float] = Field(default=None, ge=0, le=10)
    rumination_minutes: Optional[int] = Field(default=None, ge=0, le=1440)
```

### Model Registry

```python
MODEL_REGISTRY: Dict[str, type[BaseRecord]] = {
    'energy_hourly': EnergyHourlyRecord,
    'energy_daily': EnergyDailyRecord,
    'environmental_metrics': EnvironmentalMetricsRecord,
    'dairy_production': DairyProductionRecord,
}
```

---

## Analytical Views

### v_environmental_metrics_daily
Daily aggregates of environmental data with min/max/avg for each metric.

### v_air_quality_analysis
PM10/PM2.5 analysis with quality ratings (Good/Fair/Moderate/Poor).

### v_environmental_summary
Monthly environmental metric aggregates.

### v_dairy_production_daily
Daily production with calculated total production and feed efficiency.

### v_feed_efficiency_analysis
Feed efficiency trends over time.

### v_production_summary
Monthly dairy production aggregates.

### device_energy / device_weather
Backward-compatible views over `generic_device` table.

---

## Index Strategy

| Table | Index | Purpose |
|-------|-------|---------|
| `fact_energy_hourly` | `(device_id, ts)` | Time-series queries per device |
| `fact_energy_daily` | `(device_id, ts)` | Time-series queries per device |
| `environmental_metrics` | `(timestamp)` | Time-series queries |
| `dairy_production` | `(production_date)` | Date-based queries |
| `staging_*` | `(file_id)` | Batch processing by file |
| `staging_*` | `(is_valid, loaded_to_final)` | Load processing |
| `pipeline_execution` | `(file_id)` | Execution lookup |
| `pipeline_execution` | `(stage, status)` | Stage monitoring |
| `generic_device` | `(device_id)` | Device lookup |
| `generic_device` | `(device_type)` | Type filtering |
| `generic_device` | `USING GIN(metadata)` | JSONB queries |
| `api_fetch_cursor` | `(connector_id, endpoint_id, device_id)` | Cursor lookup |
| `ingest_file` | `(sha256)` | Deduplication |

---

## Conflict Resolution

Defined in YAML mappings and implemented by `DataDAO`:

```yaml
conflict_resolution:
  strategy: update           # update | ignore | fail | append
  on_columns:
    - device_id
    - ts
  update_columns:
    - energy_kwh
```

**Strategies:**
- `update`: `ON CONFLICT DO UPDATE SET ...` (upsert)
- `ignore`: `ON CONFLICT DO NOTHING`
- `fail`: No conflict clause (constraint violation)
- `append`: Same as ignore (allows duplicates with different keys)

---

## Migration History

| Version | Description |
|---------|-------------|
| V001 | Initial `ingest_file` table |
| V002 | Legacy `device` table |
| V003 | Energy fact tables |
| V004 | Analytical views |
| V005 | Dairy production tables |
| V006 | Environmental metrics |
| V007 | Pipeline metadata + staging tables |
| V008 | `generic_device` refactor |
| V009 | API fetch cursor |
