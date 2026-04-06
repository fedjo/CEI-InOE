# CEI-InOE Documentation

Welcome to the CEI-InOE project wiki. CEI-InOE is an agricultural/dairy farm data ingestion and analytics platform that collects energy, environmental, solar, and dairy production data from multiple sources and exposes it via a REST API.

---

## Platform Overview

```
External Sources                 Ingestor                  Storage              API
─────────────────                ─────────────────────     ──────────────────   ────────────
Tago.io (energy)    ──────────▶  Connectors + Pipeline ──▶  PostgreSQL 16    ──▶  FastAPI
Airbeld (weather)               (ETL, staging, load)        (fact/staging/      (REST)
FusionSolar (PV)                                             metadata tables)
CSV / XLSX files
```

Two long-running services share a single PostgreSQL 16 database:

| Service | Purpose |
|---------|---------|
| **`ingestor/`** | Pulls data from external APIs and watches `/data/incoming` for CSV/XLSX uploads; runs ETL pipelines concurrently via a thread worker pool |
| **`api/`** | FastAPI REST service for querying the data warehouse; read-only, protected by `X-API-Key` |

A **`shared/`** Python package contains the SQLAlchemy ORM models and Pydantic schemas used by both services.

---

## Documentation Index

### API
| Page | Description |
|------|-------------|
| [[API-Architecture]] | Platform diagram, directory structure, auth, request lifecycle, schema change workflow |
| [[API-Reference]] | All endpoints with query parameters and response shapes |

### Ingestor
| Page | Description |
|------|-------------|
| [[Ingestor-Architecture]] | Components, connectors, pipeline stages, worker pool, DAO layer, environment variables |
| [[Ingestor-Database-Schema]] | Full schema: all tables (fact, staging, metadata), SQL definitions, indexes, migration history |

### Operations
| Page | Description |
|------|-------------|
| [[Schema-Change-Guide]] | Step-by-step guide for adding columns, tables, and views safely |
| [[Migration-Guide]] | Flyway → Alembic migration history and procedures |

---

## Quick Start

### Docker (recommended)

```bash
# Start all services (Alembic migrations + API + Ingestor + Grafana)
docker-compose -f docker-compose.new.yaml up -d
```

### Local Development

```bash
# 1. Install shared package
pip install -e ./shared

# 2. Run the API
cd api && pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000

# 3. Run the Ingestor
cd ingestor && pip install -r requirements.txt
python app/main.py
```

### Database Migrations (Alembic)

```bash
# Apply all pending migrations
alembic upgrade head

# Generate a new migration after model changes
alembic revision --autogenerate -m "short_description"

# Rollback one step
alembic downgrade -1
```

### Run Tests

```bash
cd ingestor
pytest app/tests/
```

---

## Key Conventions

| Layer | Naming Convention | Example |
|-------|------------------|---------|
| SQLAlchemy models | `PascalCase` | `FactEnergyHourly`, `IngestBatch` |
| DB tables | `snake_case` | `fact_energy_hourly`, `ingest_batch` |
| Pydantic schemas | `PascalCase` + `Read`/`Create`/`Update` | `DatasourceRead`, `IngestBatchRead` |
| Connectors | `PascalCase` + `Connector` | `TagoConnector`, `FusionSolarConnector` |
| DAOs | `PascalCase` + `DAO` | `StagingDAO`, `BatchDAO` |
| Env vars | `UPPER_SNAKE_CASE` | `DB_DSN`, `FUSIONSOLAR_USER` |

---

## Repository

- **GitHub:** https://github.com/fedjo/CEI-InOE
- **API Docs (Swagger):** http://localhost:8000/docs *(when running locally)*
- **Grafana:** http://localhost:3000 *(when running via Docker)*
