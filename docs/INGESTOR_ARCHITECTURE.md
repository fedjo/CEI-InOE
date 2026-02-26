# CEI-InOE Ingestor Architecture

## Overview

The CEI-InOE Ingestor is a modular, connector-based data ingestion system designed to collect, validate, transform, and load data from multiple sources (files, APIs) into a PostgreSQL data warehouse. It uses a worker pool pattern with a message queue for concurrent processing and APScheduler for periodic data discovery.

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              IngestorApp                                     │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────────┐  │
│  │   APScheduler   │    │  Thread-Safe    │    │     WorkerPool          │  │
│  │                 │───▶│     Queue       │───▶│  (n worker threads)     │  │
│  │ discover jobs   │    │                 │    │                         │  │
│  └─────────────────┘    └─────────────────┘    └───────────┬─────────────┘  │
│           │                                                 │                │
│           ▼                                                 ▼                │
│  ┌────────────────────────────────────────┐   ┌────────────────────────────┐│
│  │            Connectors                   │   │      PipelineRunner        ││
│  │  ┌──────────┐ ┌──────────┐ ┌────────┐  │   │                            ││
│  │  │  File    │ │  Tago    │ │Airbeld │  │   │  ┌──────────────────────┐  ││
│  │  │Connector │ │Connector │ │Connector│  │   │  │    DataPipeline      │  ││
│  │  └──────────┘ └──────────┘ └────────┘  │   │  │  validate→transform  │  ││
│  │        │           │           │       │   │  │      →stage→load     │  ││
│  │        ▼           ▼           ▼       │   │  └──────────────────────┘  ││
│  │     InputEnvelope (standardized)       │   │             │              ││
│  └────────────────────────────────────────┘   │             ▼              ││
│                                               │  ┌──────────────────────┐  ││
│                                               │  │      DAOFactory       │  ││
│                                               │  │  (Database Access)    │  ││
│                                               │  └──────────────────────┘  ││
│                                               └────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
                           ┌──────────────────┐
                           │    PostgreSQL    │
                           │   Data Warehouse │
                           └──────────────────┘
```

## Core Components

### 1. IngestorApp (`main.py`)

The main application orchestrator that:
- Initializes all connectors from configuration
- Manages the APScheduler for periodic discovery jobs
- Creates and manages the worker pool
- Handles graceful shutdown on SIGTERM/SIGINT

```python
class IngestorApp:
    connectors: Dict[str, BaseConnector]  # Registry of active connectors
    queue: Queue                          # Thread-safe work queue
    scheduler: BackgroundScheduler        # APScheduler instance
    runner: PipelineRunner               # Pipeline execution handler
    worker_pool: WorkerPool              # Worker thread manager
```

### 2. WorkerPool (`main.py`)

A pool of daemon worker threads that process `InputEnvelope` messages from the queue:

- **Concurrency**: Configurable via `NUM_WORKERS` environment variable (default: 2)
- **Processing**: Each worker runs a loop that:
  1. Gets an envelope from the queue (blocking with timeout)
  2. Resolves the connector from envelope's `connector_id`
  3. Executes the pipeline via `PipelineRunner.run()`
  4. Calls `connector.ack()` on success or `connector.fail()` on error
- **Graceful Shutdown**: Uses threading.Event for coordinated shutdown

```
Worker Thread Lifecycle:
┌────────────────────────────────────────────────────────┐
│  while not shutdown_event.is_set():                    │
│      ├─► queue.get(timeout=1) → InputEnvelope          │
│      ├─► connector = connectors[envelope.connector_id] │
│      ├─► metrics = runner.run(envelope)                │
│      ├─► connector.ack(envelope)  # or .fail()         │
│      └─► queue.task_done()                             │
└────────────────────────────────────────────────────────┘
```

### 3. Connectors (`connectors/`)

Connectors are responsible for discovering, fetching, and wrapping data into standardized `InputEnvelope` objects. All connectors implement the `BaseConnector` interface.

#### BaseConnector Interface

```python
class BaseConnector(ABC):
    def start() -> None:       # Initialize resources
    def stop() -> None:        # Cleanup resources
    def discover() -> List[str]:  # Find available work items
    def fetch(item_id: str) -> Optional[InputEnvelope]:  # Fetch and wrap data
    def ack(envelope: InputEnvelope) -> None:   # Mark success
    def fail(envelope: InputEnvelope, error: str) -> None:  # Mark failure
```

#### InputEnvelope

A lightweight, standardized data carrier:

```python
class InputEnvelope(BaseModel):
    connector_id: str        # Source connector ID
    input_id: str           # Idempotency key (e.g., "file.csv:{sha256}")
    source_uri: str         # Origin (file path, API URL)
    received_at: datetime   # Timestamp
    content: Any            # Raw data: List[Dict], bytes
    content_type: str       # "csv", "excel", "json"
    
    # Pipeline hints
    hint_mapping: Optional[str]      # YAML mapping file path
    hint_device_id: Optional[str]    # Device identifier
    hint_granularity: Optional[str]  # "hourly", "daily"
    
    # Provenance tracking
    metadata: Dict[str, Any]  # sha256, file_size, date ranges, etc.
```

#### Available Connectors

| Connector | Type | Description | Authentication |
|-----------|------|-------------|----------------|
| `FileConnector` | file | Watches directory for CSV/Excel files | N/A |
| `TagoConnector` | tago | Tago.io energy API (hourly/daily) | Per-device token |
| `AirbeldConnector` | airbeld | Airbeld environmental sensors | OAuth email/password |
| `HttpConnector` | http | Generic REST API (base class) | Bearer, API key, OAuth |

### 4. PipelineRunner (`pipeline_runner.py`)

Orchestrates the complete ingestion workflow for a single `InputEnvelope`:

```
┌─────────────────────────────────────────────────────────────────┐
│                      PipelineRunner.run()                        │
├─────────────────────────────────────────────────────────────────┤
│  1. Check Duplicates                                             │
│     └─► IngestFileDAO.exists_by_sha256(envelope.metadata.sha256)│
│                                                                  │
│  2. Load YAML Mapping                                            │
│     └─► Parse hint_mapping file                                  │
│                                                                  │
│  3. Resolve Device                                               │
│     └─► DeviceDAO.get_by_device_id(hint_device_id)              │
│                                                                  │
│  4. Register Input                                               │
│     └─► IngestFileDAO.register() → file_id UUID                 │
│                                                                  │
│  5. Execute Pipeline                                             │
│     └─► DataPipeline(conn, mapping, context).execute(records)   │
│                                                                  │
│  6. Update Metrics                                               │
│     └─► IngestFileDAO.update_metrics(quality_score, etc.)       │
└─────────────────────────────────────────────────────────────────┘
```

### 5. DataPipeline (`pipeline.py`)

The core ETL engine that processes records through staging:

```
Pipeline Stages:
┌──────────────────────────────────────────────────────────────────┐
│  EXTRACT  │  Records already parsed by connector (CSV rows/API)  │
├───────────┼──────────────────────────────────────────────────────┤
│  VALIDATE │  PydanticTransformer validates + transforms          │
│     +     │  - Column mapping (CSV → DB columns)                 │
│ TRANSFORM │  - Type coercion (European decimals, AM/PM dates)    │
│     +     │  - Constraint validation (min/max ranges)            │
│   STAGE   │  - Insert to staging table (raw + transformed)       │
├───────────┼──────────────────────────────────────────────────────┤
│   LOAD    │  Move valid records from staging → final table       │
│           │  - Conflict resolution (update/ignore/fail)          │
│           │  - Device ID injection for fact tables               │
└───────────┴──────────────────────────────────────────────────────┘
```

### 6. PydanticTransformer (`pydantic_transformer.py`)

Unified validation and transformation using Pydantic models:

- **Column Mapping**: Maps source columns (CSV headers) to database columns via YAML config
- **Type Coercion**: Handles European decimals (`1.234,56`), AM/PM dates, integer-from-float
- **Constraint Validation**: Uses Pydantic `Field(ge=, le=, ...)` for range validation
- **Error Collection**: Converts Pydantic errors to `ValidationResult` for staging

### 7. DAO Layer (`dao/`)

Centralized database access through the `DAOFactory`:

```python
dao = DAOFactory(connection)
dao.device         # DeviceDAO - device lookup
dao.ingest_file    # IngestFileDAO - file registration/dedup
dao.staging('energy_hourly')  # StagingDAO - staging operations
dao.data(conflict_config)     # DataDAO - final table inserts
dao.pipeline       # PipelineDAO - execution logging
dao.cursor         # CursorDAO - API cursor tracking
```

## Data Flow

### File Ingestion Flow

```
┌────────────┐    ┌────────────────┐    ┌──────────────┐
│ /data/     │    │ FileConnector  │    │   Queue      │
│ incoming/  │───▶│   .discover()  │───▶│              │
│ file.csv   │    │   .fetch()     │    │ InputEnvelope│
└────────────┘    └────────────────┘    └──────┬───────┘
                                               │
                    ┌──────────────────────────┘
                    ▼
             ┌──────────────┐
             │Worker Thread │
             │              │
             │ runner.run() │
             └──────┬───────┘
                    │
      ┌─────────────┼─────────────┐
      ▼             ▼             ▼
┌──────────┐  ┌──────────┐  ┌──────────┐
│ staging_ │  │ staging_ │  │ staging_ │
│ energy   │  │ environ  │  │ dairy    │
│ hourly   │  │ metrics  │  │ prod     │
└────┬─────┘  └────┬─────┘  └────┬─────┘
     │             │             │
     ▼             ▼             ▼
┌──────────┐  ┌──────────┐  ┌──────────┐
│ fact_    │  │ environ  │  │ dairy_   │
│ energy   │  │ mental   │  │ produc   │
│ hourly   │  │ _metrics │  │ tion     │
└──────────┘  └──────────┘  └──────────┘
                    │
                    ▼
            ┌──────────────┐
            │ /data/       │
            │ processed/   │
            │ file.csv     │
            └──────────────┘
```

### API Ingestion Flow (Tago.io Example)

```
┌────────────────┐    ┌────────────────┐    ┌──────────────┐
│   APScheduler  │    │ TagoConnector  │    │   Queue      │
│   every 1 hr   │───▶│  .discover()   │───▶│              │
│                │    │  per device    │    │ InputEnvelope│
└────────────────┘    └───────┬────────┘    └──────────────┘
                              │
                              │ For each device:
                              │ ┌───────────────────────────┐
                              │ │ 1. Get cursor timestamp   │
                              │ │ 2. Query API with range   │
                              │ │ 3. Transform response     │
                              │ │ 4. Create InputEnvelope   │
                              │ │ 5. Update cursor          │
                              │ └───────────────────────────┘
                              │
                              ▼
                     ┌────────────────┐
                     │  Tago.io API   │
                     │ /data?start=.. │
                     │ device-token   │
                     └────────────────┘
```

## Design Principles

### 1. Connector Abstraction
Connectors encapsulate all source-specific logic (authentication, data format, ack/fail semantics). The pipeline only sees standardized `InputEnvelope` objects.

### 2. Staging-First Pattern
All records pass through staging tables before final tables:
- Raw data preserved for debugging
- Validation errors stored for inspection
- Atomic loads with rollback capability
- Audit trail via `loaded_to_final` flag

### 3. Idempotency
- Files: SHA256 hash stored in `ingest_file.sha256`
- APIs: Cursor tracking in `api_fetch_cursor` table
- Pipeline: `DuplicateInputError` raised for known inputs

### 4. Configuration-Driven Mappings
YAML mappings define:
- Column/field mappings
- Type coercions
- Validation constraints
- Conflict resolution strategy
- Target/staging table names

### 5. Observable Pipeline
- `pipeline_execution` table logs stage start/end times
- `data_quality_checks` stores validation error samples
- `PipelineMetrics` dataclass tracks all execution stats

### 6. Graceful Degradation
- Per-record error handling (invalid records don't block valid ones)
- Connector-level isolation (one failing connector doesn't affect others)
- Automatic retries with exponential backoff for HTTP connectors

## Directory Structure

```
ingestor/
├── Dockerfile                 # Container image definition
├── requirements.txt           # Python dependencies
└── app/
    ├── main.py               # Application entry point, WorkerPool
    ├── config.py             # Environment-based configuration
    ├── pipeline.py           # DataPipeline orchestrator
    ├── pipeline_runner.py    # InputEnvelope → Pipeline execution
    ├── models.py             # Pydantic data models (BaseRecord, EnergyHourlyRecord, etc.)
    ├── pydantic_transformer.py # Unified validation + transformation
    ├── validation.py         # ValidationResult, SchemaValidator, TypeValidator
    ├── staging.py            # Legacy wrapper (deprecated, use DAOs)
    │
    ├── connectors/           # Data source connectors
    │   ├── __init__.py
    │   ├── base.py           # BaseConnector, InputEnvelope, ConnectorStatus
    │   ├── registry.py       # create_connector() factory
    │   ├── file_connector.py # CSV/Excel file watcher
    │   ├── http_connector.py # Generic REST API client
    │   ├── tago_connector.py # Tago.io energy API
    │   └── airbeld_connector.py # Airbeld environmental API
    │
    ├── dao/                  # Data Access Objects
    │   ├── __init__.py       # Public exports
    │   ├── base.py           # BaseDAO with cursor management
    │   ├── factory.py        # DAOFactory for centralized access
    │   ├── device_dao.py     # generic_device operations
    │   ├── ingest_file_dao.py # File registration + dedup
    │   ├── staging_dao.py    # Staging table operations
    │   ├── data_dao.py       # Final table inserts with conflict resolution
    │   ├── pipeline_dao.py   # Execution + quality logging
    │   └── cursor_dao.py     # API cursor tracking
    │
    ├── mappings/             # YAML dataset configurations
    │   ├── energy_hourly.yaml
    │   ├── energy_daily.yaml
    │   ├── environmental_metrics.yaml
    │   ├── dairy_production.yaml
    │   ├── api_energy_hourly.yaml
    │   ├── api_energy_daily.yaml
    │   └── api_environmental_metrics.yaml
    │
    └── tests/                # Unit tests
        ├── test_models.py
        ├── test_pipeline.py
        └── test_transformer.py
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_DSN` | - | PostgreSQL connection string |
| `NUM_WORKERS` | 2 | Number of worker threads |
| `QUEUE_MAX_SIZE` | 100 | Maximum queue capacity |
| `LOG_LEVEL` | INFO | Logging verbosity |
| `WATCH_DIR` | /data/incoming | File connector watch directory |
| `PROCESSED_DIR` | /data/processed | Processed files directory |
| `REJECTED_DIR` | /data/rejected | Failed files directory |
| `TAGO_ENABLED` | true | Enable Tago.io connector |
| `TAGO_POLL_INTERVAL` | 3600 | Tago polling interval (seconds) |
| `AIRBELD_POLL_INTERVAL` | 43200 | Airbeld polling interval (seconds) |

### YAML Mapping Example

```yaml
dataset: energy_hourly
granularity: hourly

# Column mapping: source → database
columns:
  "Date and Time": ts
  "Hourly": energy_kwh

# Type coercions
coercions:
  ts: datetime
  energy_kwh: float

# Validation rules
validation_rules:
  schema:
    required:
      - ts
      - energy_kwh
  constraints:
    energy_kwh:
      min: 0
      max: 10000

# Conflict resolution
conflict_resolution:
  strategy: update           # update | ignore | fail | append
  on_columns:
    - device_id
    - ts
  update_columns:
    - energy_kwh

# Target tables
target_table: fact_energy_hourly
staging_table: staging_energy_hourly
```

## Adding a New Connector

1. Create connector class extending `BaseConnector` or `HttpConnector`
2. Implement required methods: `start()`, `stop()`, `discover()`, `fetch()`, `ack()`, `fail()`
3. Register in `connectors/registry.py`:
   ```python
   CONNECTOR_TYPES['myapi'] = MyApiConnector
   ```
4. Add configuration in `config.py`:
   ```python
   CONNECTOR_CONFIGS['my_connector'] = {
       'type': 'myapi',
       'schedule_seconds': 3600,
       ...
   }
   ```

## Adding a New Dataset

1. Create Pydantic model in `models.py` extending `BaseRecord`
2. Register in `MODEL_REGISTRY`
3. Create YAML mapping in `mappings/`
4. Add staging table mapping in `StagingDAO.STAGING_TABLES`
5. Create database migrations for staging and final tables
