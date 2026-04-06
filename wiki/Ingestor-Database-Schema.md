[[Home]] | [[API-Architecture]] | [[API-Reference]] | [[Ingestor-Architecture]] | [[Ingestor-Database-Schema]] | [[Schema-Change-Guide]] | [[Migration-Guide]]
---

# CEI-InOE Database Schema

## Overview

The CEI-InOE data warehouse uses PostgreSQL 16 with a layered architecture:
- **Fact Tables**: Final analytical data (energy, environmental, dairy, solar)
- **Staging Tables**: Intermediate validation/transformation layer
- **Metadata Tables**: Pipeline execution, quality tracking, datasource registry
- **Site Table**: Physical site management

Migrations are managed by **Alembic** (`alembic/versions/`).

## Entity Relationship Diagram

```
                                    ┌──────────────────────┐
                                    │         site         │
                                    │  (physical locations)│
                                    └──────────┬───────────┘
                                               │ 1:N
                                               ▼
                                    ┌──────────────────────┐
                                    │      datasource      │
                                    │  (unified registry)  │
                                    └──────────┬───────────┘
                                               │ 1:N
                                               ▼
                                    ┌──────────────────────┐
                                    │     ingest_batch     │
                                    │  (ingestion tracking)│
                                    └──────────┬───────────┘
                                               │ 1:N
              ┌────────────────────────────────┼────────────────────────────────┐
              │                                │                                │
              ▼                                ▼                                ▼
┌──────────────────────┐    ┌──────────────────────────┐    ┌──────────────────────────┐
│  staging_energy_*    │    │   pipeline_execution     │    │   data_quality_checks    │
│  staging_environ_*   │    │   (stage tracking)       │    │   (validation results)   │
│  staging_dairy_*     │    └──────────────────────────┘    └──────────────────────────┘
└──────────┬───────────┘
           │ loads into
           ▼
┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
│  fact_energy_hourly  │  │  fact_energy_daily   │  │ environmental_metrics│
│  fact_solar_hourly   │  │  fact_solar_daily    │  │   dairy_production   │
│                      │  │  fact_solar_monthly  │  │                      │
└──────────────────────┘  └──────────────────────┘  └──────────────────────┘

                                    ┌──────────────────────┐
                                    │   api_fetch_cursor   │
                                    │   (incremental fetch)│
                                    └──────────────────────┘
```

---

## Core Tables

### site

Physical site registry (dairy farms, factories).

```sql
CREATE TABLE site (
    id                   SERIAL PRIMARY KEY,
    name                 VARCHAR(255) NOT NULL,
    location             JSONB,                       -- GeoJSON coordinates
    site_type            VARCHAR(32),                 -- 'dairy_farm', 'factory', 'other'
    owner                JSONB,                       -- Owner contact details
    administrator_email  VARCHAR(255),
    created_at           TIMESTAMPTZ DEFAULT NOW(),
    updated_at           TIMESTAMPTZ DEFAULT NOW()
);
```

### datasource

Unified registry for all data sources: API devices, file upload sources, and manual entries.
Replaces the legacy `generic_device` and `ingest_file` device references.

```sql
CREATE TABLE datasource (
    id               SERIAL PRIMARY KEY,
    external_id      VARCHAR(64) NOT NULL UNIQUE,  -- API device ID, station ID, etc.
    source_category  VARCHAR(32) NOT NULL,          -- 'device', 'file', 'api', 'manual'
    data_type        VARCHAR(32) NOT NULL,           -- 'energy', 'weather', 'dairy', 'pv'
    name             VARCHAR(255),
    alias            VARCHAR(255),
    client           VARCHAR(255) NOT NULL,          -- Owner/customer name
    description      TEXT,
    status           VARCHAR(32) DEFAULT 'active',  -- 'active', 'inactive', 'deprecated'
    timezone         VARCHAR(64) DEFAULT 'UTC',
    metadata_        JSONB DEFAULT '{}',             -- Type-specific config (tokens, IDs)
    site_id          INTEGER REFERENCES site(id),
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    updated_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_datasource_external_id ON datasource(external_id);
CREATE INDEX idx_datasource_data_type   ON datasource(data_type);
CREATE INDEX idx_datasource_category    ON datasource(source_category);
CREATE INDEX idx_datasource_status      ON datasource(status);
CREATE INDEX idx_datasource_client      ON datasource(client);
CREATE INDEX idx_datasource_metadata    ON datasource USING GIN(metadata_);
```

**Metadata examples:**
- Tago.io energy device: `{"device_token": "abc123", "tago_device_id": "68f1..."}`
- Airbeld weather station: `{"station_id": "1236", "location": "LK Dairy Farm"}`
- FusionSolar PV plant: `{"plant_code": "NE=146462613"}`

### ingest_batch

Ingestion batch tracking: one batch per file upload or per API fetch cycle per device.
Replaces the legacy `ingest_file` table.

```sql
CREATE TABLE ingest_batch (
    batch_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_type       VARCHAR(32) NOT NULL,          -- 'file', 'api', 'stream', 'manual'
    source_name       TEXT NOT NULL,                  -- Filename or API endpoint URL
    datasource_id     INTEGER REFERENCES datasource(id),
    file_sha256       VARCHAR(64) UNIQUE,             -- File deduplication key
    granularity       VARCHAR(32),                    -- 'hourly', 'daily', 'monthly'
    date_range_start  DATE,
    date_range_end    DATE,
    status            VARCHAR(32) DEFAULT 'pending', -- 'pending','processing','completed','failed'
    records_loaded    INTEGER DEFAULT 0,
    records_failed    INTEGER DEFAULT 0,
    execution_time_ms INTEGER,
    validation_status VARCHAR(32),                   -- 'passed', 'failed', 'partial'
    quality_score     NUMERIC(5,2),                  -- Percentage of valid records
    pipeline_version  VARCHAR(32) DEFAULT '1.0',
    started_at        TIMESTAMPTZ DEFAULT NOW(),
    completed_at      TIMESTAMPTZ
);

CREATE INDEX idx_ingest_batch_source_type  ON ingest_batch(source_type);
CREATE INDEX idx_ingest_batch_status       ON ingest_batch(status);
CREATE INDEX idx_ingest_batch_datasource   ON ingest_batch(datasource_id);
CREATE INDEX idx_ingest_batch_validation   ON ingest_batch(validation_status, started_at);
```

---

## Fact Tables

### fact_energy_hourly

Hourly energy consumption per datasource.

```sql
CREATE TABLE fact_energy_hourly (
    energy_id           SERIAL PRIMARY KEY,
    ts                  TIMESTAMP NOT NULL,
    energy_kwh          FLOAT NOT NULL,
    source_type         VARCHAR(32) DEFAULT 'csv',     -- 'csv', 'api', 'excel'
    source_batch_id     UUID REFERENCES ingest_batch(batch_id),
    source_api_endpoint TEXT,
    source_device_id    VARCHAR(64),                   -- External device ID string
    ingested_at         TIMESTAMPTZ DEFAULT NOW(),
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source_batch_id, ts)
);

CREATE INDEX idx_energy_hourly_ts     ON fact_energy_hourly(ts);
CREATE INDEX idx_energy_hourly_source ON fact_energy_hourly(source_type, source_device_id);
CREATE INDEX idx_energy_hourly_batch  ON fact_energy_hourly(source_batch_id);
```

### fact_energy_daily

Daily energy consumption per datasource.

```sql
CREATE TABLE fact_energy_daily (
    energy_id           SERIAL PRIMARY KEY,
    ts                  DATE NOT NULL,
    energy_kwh          FLOAT NOT NULL,
    source_type         VARCHAR(32) DEFAULT 'csv',
    source_batch_id     UUID REFERENCES ingest_batch(batch_id),
    source_api_endpoint TEXT,
    source_device_id    VARCHAR(64),
    ingested_at         TIMESTAMPTZ DEFAULT NOW(),
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source_batch_id, ts)
);

CREATE INDEX idx_energy_daily_ts     ON fact_energy_daily(ts);
CREATE INDEX idx_energy_daily_source ON fact_energy_daily(source_type, source_device_id);
CREATE INDEX idx_energy_daily_batch  ON fact_energy_daily(source_batch_id);
```

### environmental_metrics

Environmental sensor readings (weather station data).

```sql
CREATE TABLE environmental_metrics (
    id                     SERIAL PRIMARY KEY,
    timestamp              TIMESTAMPTZ NOT NULL,
    atm_pressure           NUMERIC(8, 2),             -- Atmospheric pressure (hPa)
    noise_level_db         NUMERIC(6, 2),             -- Noise level (dB)
    temperature            NUMERIC(6, 2),             -- Temperature (°C)
    humidity               NUMERIC(6, 2),             -- Relative humidity (%)
    pm10                   NUMERIC(8, 2),             -- PM10 particulate (µg/m³)
    wind_speed             NUMERIC(6, 2),             -- Wind speed (m/s)
    wind_direction_sectors NUMERIC(6, 2),             -- Wind direction (sectors)
    wind_angle             NUMERIC(6, 2),             -- Wind angle (degrees)
    pm2p5                  NUMERIC(8, 2),             -- PM2.5 particulate (µg/m³)
    source_type            VARCHAR(32) DEFAULT 'csv',
    source_batch_id        UUID REFERENCES ingest_batch(batch_id),
    source_api_endpoint    TEXT,
    source_device_id       VARCHAR(64),
    ingested_at            TIMESTAMPTZ DEFAULT NOW(),
    created_at             TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source_batch_id, timestamp)
);

CREATE INDEX idx_env_metrics_timestamp ON environmental_metrics(timestamp);
CREATE INDEX idx_env_metrics_source    ON environmental_metrics(source_type, source_device_id);
CREATE INDEX idx_env_metrics_batch     ON environmental_metrics(source_batch_id);
```

### dairy_production

Daily dairy farm production metrics.

```sql
CREATE TABLE dairy_production (
    id                        SERIAL PRIMARY KEY,
    production_date           DATE NOT NULL,
    day_production_per_cow_kg NUMERIC(10, 2),          -- Milk per cow (kg)
    number_of_animals         INTEGER,
    average_lactation_days    INTEGER,
    fed_per_cow_total_kg      NUMERIC(10, 2),           -- Feed per cow (kg)
    fed_per_cow_water_kg      NUMERIC(10, 2),           -- Water per cow (kg)
    feed_efficiency           NUMERIC(10, 4),           -- Milk/feed ratio
    rumination_minutes        INTEGER,
    source_type               VARCHAR(32) DEFAULT 'csv',
    source_batch_id           UUID REFERENCES ingest_batch(batch_id),
    source_api_endpoint       TEXT,
    source_device_id          VARCHAR(64),
    ingested_at               TIMESTAMPTZ DEFAULT NOW(),
    created_at                TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_dairy_production_date  ON dairy_production(production_date);
CREATE INDEX idx_dairy_production_batch ON dairy_production(source_batch_id);
```

### fact_solar_hourly

Hourly solar power generation from FusionSolar.

```sql
CREATE TABLE fact_solar_hourly (
    solar_id            SERIAL PRIMARY KEY,
    ts                  TIMESTAMP NOT NULL,
    power_kw            FLOAT NOT NULL,                 -- Instantaneous power (kW)
    source_type         VARCHAR(32) DEFAULT 'api',
    source_batch_id     UUID REFERENCES ingest_batch(batch_id),
    source_api_endpoint TEXT,
    source_device_id    VARCHAR(64),
    ingested_at         TIMESTAMPTZ DEFAULT NOW(),
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source_batch_id, ts)
);

CREATE INDEX idx_solar_hourly_ts    ON fact_solar_hourly(ts);
CREATE INDEX idx_solar_hourly_batch ON fact_solar_hourly(source_batch_id);
```

### fact_solar_daily

Daily solar energy generation.

```sql
CREATE TABLE fact_solar_daily (
    solar_id            SERIAL PRIMARY KEY,
    ts                  DATE NOT NULL,
    energy_kwh          FLOAT NOT NULL,
    source_type         VARCHAR(32) DEFAULT 'api',
    source_batch_id     UUID REFERENCES ingest_batch(batch_id),
    source_api_endpoint TEXT,
    source_device_id    VARCHAR(64),
    ingested_at         TIMESTAMPTZ DEFAULT NOW(),
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source_batch_id, ts)
);

CREATE INDEX idx_solar_daily_ts    ON fact_solar_daily(ts);
CREATE INDEX idx_solar_daily_batch ON fact_solar_daily(source_batch_id);
```

### fact_solar_monthly

Monthly solar energy generation.

```sql
CREATE TABLE fact_solar_monthly (
    solar_id            SERIAL PRIMARY KEY,
    ts                  DATE NOT NULL,
    energy_kwh          FLOAT NOT NULL,
    source_type         VARCHAR(32) DEFAULT 'api',
    source_batch_id     UUID REFERENCES ingest_batch(batch_id),
    source_api_endpoint TEXT,
    source_device_id    VARCHAR(64),
    ingested_at         TIMESTAMPTZ DEFAULT NOW(),
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source_batch_id, ts)
);

CREATE INDEX idx_solar_monthly_ts    ON fact_solar_monthly(ts);
CREATE INDEX idx_solar_monthly_batch ON fact_solar_monthly(source_batch_id);
```

---

## Staging Tables

Each staging table mirrors its target fact table and adds validation tracking columns. Data flows through staging before being promoted to the final table.

```sql
-- Pattern applies to all staging tables:
-- staging_energy_hourly, staging_energy_daily,
-- staging_environmental_metrics, staging_dairy_production

CREATE TABLE staging_energy_hourly (
    staging_id        SERIAL PRIMARY KEY,
    batch_id          UUID REFERENCES ingest_batch(batch_id),
    row_number        INTEGER NOT NULL,                -- Source row for debugging
    raw_data          JSONB NOT NULL,                  -- Original CSV/API record
    transformed_data  JSONB,                           -- Post-transformation data
    validation_errors JSONB,                           -- List of validation error objects
    is_valid          BOOLEAN DEFAULT FALSE,
    loaded_to_final   BOOLEAN DEFAULT FALSE,
    created_at        TIMESTAMPTZ DEFAULT NOW()
);
```

**Staging tables:**
- `staging_energy_hourly`
- `staging_energy_daily`
- `staging_environmental_metrics`
- `staging_dairy_production`

**Mapping in Code (StagingDAO):**
```python
STAGING_TABLES = {
    'energy_hourly': 'staging_energy_hourly',
    'energy_daily': 'staging_energy_daily',
    'environmental_metrics': 'staging_environmental_metrics',
    'dairy_production': 'staging_dairy_production',
}
```

---

## Pipeline Metadata Tables

### pipeline_execution

Tracks each stage of pipeline execution for observability and debugging.

```sql
CREATE TABLE pipeline_execution (
    id                  SERIAL PRIMARY KEY,
    ingest_batch_id     UUID REFERENCES ingest_batch(batch_id),
    pipeline_name       TEXT NOT NULL,               -- e.g., 'energy_hourly_pipeline'
    stage               TEXT NOT NULL,               -- 'extract', 'validate', 'transform', 'load'
    started_at          TIMESTAMPTZ DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    status              TEXT NOT NULL,               -- 'running', 'success', 'failed', 'skipped'
    records_in          INTEGER DEFAULT 0,
    records_out         INTEGER DEFAULT 0,
    error_message       TEXT,
    execution_metadata  JSONB,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_pipeline_batch        ON pipeline_execution(ingest_batch_id);
CREATE INDEX idx_pipeline_stage_status ON pipeline_execution(stage, status);
```

### data_quality_checks

Records data quality validation results per batch.

```sql
CREATE TABLE data_quality_checks (
    id              SERIAL PRIMARY KEY,
    ingest_batch_id UUID REFERENCES ingest_batch(batch_id),
    dataset         TEXT NOT NULL,               -- 'energy_hourly', 'environmental_metrics'
    check_type      TEXT NOT NULL,               -- 'schema', 'type', 'range', 'uniqueness'
    check_name      TEXT NOT NULL,               -- 'temperature_range', 'required_fields'
    passed          BOOLEAN NOT NULL,
    failed_count    INTEGER DEFAULT 0,
    total_count     INTEGER DEFAULT 0,
    failure_rate    NUMERIC(5, 2),               -- Percentage
    sample_failures JSONB,                       -- Example failing records for debugging
    checked_at      TIMESTAMPTZ DEFAULT NOW()
);
```

### api_fetch_cursor

Tracks last successful API fetch timestamp for incremental (cursor-based) fetching.
Each row is unique per (connector, endpoint, device) combination.

```sql
CREATE TABLE api_fetch_cursor (
    id                   SERIAL PRIMARY KEY,
    connector_id         TEXT NOT NULL,           -- 'tago_energy', 'airbeld_environmental'
    endpoint_id          TEXT NOT NULL,           -- 'hourly_consumption', 'diffday'
    device_id            TEXT NOT NULL,           -- External device/station ID
    datasource_id        INTEGER REFERENCES datasource(id),
    last_fetch_timestamp TIMESTAMPTZ NOT NULL,    -- Timestamp of last fetched record
    last_fetch_success   TIMESTAMPTZ DEFAULT NOW(),
    fetch_count          INTEGER DEFAULT 1,
    created_at           TIMESTAMPTZ DEFAULT NOW(),
    updated_at           TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(connector_id, endpoint_id, device_id)
);

CREATE INDEX idx_api_cursor_lookup ON api_fetch_cursor(connector_id, endpoint_id, device_id);
```

---

## Pydantic Data Models

Located in `ingestor/app/models.py`, these provide type-safe Python representations used during pipeline validation.

### BaseRecord

```python
class BaseRecord(BaseModel):
    source_type: Optional[SourceType] = SourceType.UNKNOWN  # csv, api, excel
    source_batch_id: Optional[UUID] = None
    source_api_endpoint: Optional[str] = None
    source_device_id: Optional[str] = None
    ingested_at: Optional[datetime] = None
```

### EnergyHourlyRecord

```python
class EnergyHourlyRecord(BaseRecord):
    ts: datetime                                     # TIMESTAMP NOT NULL
    energy_kwh: float = Field(ge=0, le=10000)        # kWh, 0-10000

    @field_validator('ts', mode='before')
    def validate_ts(cls, v):
        return parse_datetime_with_ampm(v)           # Handles "MM/DD/YYYY HH:MM:SS AM"

    @field_validator('energy_kwh', mode='before')
    def validate_energy_kwh(cls, v):
        return parse_european_float(v)               # Handles "1.234,56"
```

### EnergyDailyRecord

```python
class EnergyDailyRecord(BaseRecord):
    ts: date
    energy_kwh: float = Field(ge=0, le=100000)
```

### EnvironmentalMetricsRecord

```python
class EnvironmentalMetricsRecord(BaseRecord):
    timestamp: datetime
    temperature: float = Field(ge=-50, le=60)        # °C
    humidity: Optional[float] = Field(default=None, ge=0, le=100)
    atm_pressure: Optional[float] = None
    noise_level_db: Optional[float] = Field(default=None, ge=0, le=140)
    pm10: Optional[float] = Field(default=None, ge=0, le=1000)
    pm2p5: Optional[float] = Field(default=None, ge=0, le=500)
    wind_speed: Optional[float] = Field(default=None, ge=0, le=150)
    wind_angle: Optional[float] = Field(default=None, ge=0, le=360)
    wind_direction_sectors: Optional[float] = None
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

### SolarHourlyRecord / SolarDailyRecord / SolarMonthlyRecord

```python
class SolarHourlyRecord(BaseRecord):
    ts: datetime
    power_kw: float = Field(ge=0, le=10000)          # Instantaneous power (kW)

class SolarDailyRecord(BaseRecord):
    ts: date
    energy_kwh: float = Field(ge=0, le=100000)

class SolarMonthlyRecord(BaseRecord):
    ts: date
    energy_kwh: float = Field(ge=0, le=1000000)
```

### Model Registry

```python
MODEL_REGISTRY: Dict[str, type[BaseRecord]] = {
    'energy_hourly': EnergyHourlyRecord,
    'energy_daily': EnergyDailyRecord,
    'environmental_metrics': EnvironmentalMetricsRecord,
    'dairy_production': DairyProductionRecord,
    'solar_hourly': SolarHourlyRecord,
    'solar_daily': SolarDailyRecord,
    'solar_monthly': SolarMonthlyRecord,
}
```

---

## Index Strategy

| Table | Index | Purpose |
|-------|-------|---------|
| `fact_energy_hourly` | `(ts)` | Time-range queries |
| `fact_energy_hourly` | `(source_type, source_device_id)` | Device-filtered queries |
| `fact_energy_hourly` | `(source_batch_id)` | Batch lookup |
| `fact_energy_daily` | same pattern as hourly | — |
| `environmental_metrics` | `(timestamp)` | Time-range queries |
| `environmental_metrics` | `(source_type, source_device_id)` | Station-filtered queries |
| `dairy_production` | `(production_date)` | Date-based queries |
| `fact_solar_*` | `(ts)`, `(source_batch_id)` | Time + batch queries |
| `staging_*` | `(batch_id)` | Batch processing |
| `staging_*` | `(is_valid, loaded_to_final)` | Load processing |
| `pipeline_execution` | `(ingest_batch_id)` | Execution lookup |
| `pipeline_execution` | `(stage, status)` | Stage monitoring |
| `datasource` | `(external_id)` | Device lookup |
| `datasource` | `(data_type)`, `(source_category)`, `(status)` | Filtering |
| `datasource` | `GIN(metadata_)` | JSONB queries |
| `api_fetch_cursor` | `(connector_id, endpoint_id, device_id)` | Cursor lookup |
| `ingest_batch` | `(source_type)`, `(status)`, `(datasource_id)` | Filtering |

---

## Conflict Resolution

Defined in YAML mappings and implemented by `DataDAO`:

```yaml
conflict_resolution:
  strategy: update           # update | ignore | fail | append
  on_columns:
    - source_batch_id
    - ts
  update_columns:
    - energy_kwh
```

**Strategies:**
- `update`: `ON CONFLICT DO UPDATE SET ...` (upsert — most common)
- `ignore`: `ON CONFLICT DO NOTHING`
- `fail`: No conflict clause (constraint violation raises error)
- `append`: Same as ignore (for tables without unique constraints)

---

## Migration History

Managed by Alembic (`alembic/versions/`). Filenames follow `YYYYMMDD_HHMM_<description>.py`.

| Revision | File | Description |
|----------|------|-------------|
| 001 | `20260226_0001_…_initial_schema.py` | All core tables: datasource, ingest_batch, fact_*, staging_*, pipeline_execution, data_quality_checks, api_fetch_cursor |
| 002 | `20260306_1415_…_polish_models.py` | Schema refinements and index additions |
| 003 | `20260313_1321_…_relate_cursor_to_datasource.py` | FK from api_fetch_cursor → datasource |
| 004 | `20260316_1700_…_add_composite_unique_constraints.py` | Composite UNIQUE constraints on fact tables |
| 005 | `20260318_1400_…_add_site_table.py` | site table + FK from datasource → site |
| 006 | `20260319_1000_…_add_solar_tables.py` | fact_solar_hourly, fact_solar_daily, fact_solar_monthly |

To apply all migrations:
```bash
alembic upgrade head
```
