[[Home]] | [[API-Architecture]] | [[API-Reference]] | [[Ingestor-Architecture]] | [[Ingestor-Database-Schema]] | [[Schema-Change-Guide]] | [[Migration-Guide]]
---

# CEI-InOE API Architecture

## Overview

The API service provides RESTful access to environmental, energy, dairy production, and solar data collected by the CEI-InOE platform. It serves as the primary interface for external applications, dashboards, and integrations.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        CEI-InOE Platform                            │
│                                                                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────────┐  ┌────────────────┐ │
│  │  Tago.io │  │ Airbeld  │  │ FusionSolar  │  │  File Sources  │ │
│  │ (energy) │  │ (weather)│  │   (solar PV) │  │  (CSV / XLSX)  │ │
│  └────┬─────┘  └────┬─────┘  └──────┬───────┘  └───────┬────────┘ │
│       └─────────────┴───────────────┴──────────────────┘          │
│                                   ▼                                │
│                          ┌─────────────────┐                       │
│                          │    Ingestor     │                       │
│                          │   (Scheduled)   │                       │
│                          └────────┬────────┘                       │
│                                   │ writes                         │
│                                   ▼                                │
│  ┌──────────────┐      ┌─────────────────┐      ┌──────────────┐  │
│  │   Alembic    │─────▶│   PostgreSQL    │◀─────│   Grafana    │  │
│  │ (migrations) │      │                 │      │ (dashboards) │  │
│  └──────────────┘      │  - Fact tables  │      └──────────────┘  │
│                        │  - Staging      │                        │
│                        │  - Metadata     │                        │
│                        └────────┬────────┘                        │
│                                 │ reads                            │
│                                 ▼                                  │
│                        ┌─────────────────┐      ┌──────────────┐  │
│                        │      API        │◀─────│  Frontend /  │  │
│                        │   (FastAPI)     │      │  Dashboards  │  │
│                        └─────────────────┘      └──────────────┘  │
└────────────────────────────────────────────────────────────────────┘
```

## Directory Structure

```
api/
├── Dockerfile
├── requirements.txt
├── app/
│   ├── main.py                 # FastAPI app, router registration, lifespan
│   ├── config.py               # Pydantic Settings (env vars)
│   ├── auth.py                 # X-API-Key verification dependency
│   │
│   ├── db/
│   │   ├── session.py          # SQLAlchemy engine + session factory
│   │   └── queries/
│   │       ├── energy.py       # Energy data queries
│   │       ├── environmental.py
│   │       ├── dairy.py
│   │       ├── datasources.py
│   │       └── batches.py
│   │
│   └── routers/
│       ├── health.py           # GET /health, GET /ready  (public)
│       ├── energy.py           # /api/v1/energy/*
│       ├── environmental.py    # /api/v1/environmental/*
│       ├── dairy.py            # /api/v1/dairy/*
│       ├── datasources.py      # /api/v1/datasources/*
│       └── batches.py          # /api/v1/batches/*
```

> **Note:** Response schemas are defined in `shared/src/shared/schemas.py` and imported by both the API and ingestor. The API has no local `schemas/` directory.

## Authentication

All data endpoints (`/api/v1/*`) require an `X-API-Key` header. Health endpoints (`/health`, `/ready`) are public.

```
X-API-Key: <value of API_KEY env var>
```

The key is validated in `api/app/auth.py` via a FastAPI dependency injected at the router level in `main.py`. Missing or mismatched keys return `403 Forbidden`.

## Design Principles

| Principle | Implementation |
|-----------|----------------|
| **Database as Contract** | PostgreSQL schema (via Alembic migrations) is the source of truth |
| **Read-Only Access** | API only reads data; ingestor handles all writes |
| **SQLAlchemy ORM Queries** | Query modules use SQLAlchemy ORM (`Session`) for type-safe queries |
| **Shared Schemas** | Pydantic v2 models in `shared/` are used for both API responses and ingestor validation |
| **Stateless** | No session state; scales horizontally |

## Data Flow

### Request Lifecycle

```
HTTP Request
     │
     ▼
┌─────────────────────────────────────────────────────────────┐
│ Router                                                       │
│ - Validate query parameters (Pydantic / FastAPI)            │
│ - Verify X-API-Key (dependency)                             │
└─────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────┐
│ Query Function (db/queries/*.py)                             │
│ - Build SQLAlchemy ORM query                                │
│ - Execute against session (connection pool)                 │
│ - Return ORM objects or scalar results                      │
└─────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────┐
│ PostgreSQL                                                   │
│ - Query fact / metadata tables                              │
│ - Return result set                                         │
└─────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────┐
│ Schema Serialisation                                         │
│ - Pydantic model validates and serialises response          │
│ - model_validate(orm_obj) converts ORM → schema             │
└─────────────────────────────────────────────────────────────┘
     │
     ▼
HTTP Response (JSON)
```

## Service Interaction

```
┌─────────────────────────────────────────────────────────────────┐
│                     Schema Change Workflow                      │
│                                                                 │
│  Developer                                                      │
│      │                                                          │
│      ▼                                                          │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ 1. Update shared/src/shared/models.py                    │   │
│  │    Add/change SQLAlchemy model                           │   │
│  └─────────────────────────────────────────────────────────┘   │
│      │                                                          │
│      ▼                                                          │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ 2. Generate Alembic Migration                            │   │
│  │    alembic revision --autogenerate -m "description"     │   │
│  └─────────────────────────────────────────────────────────┘   │
│      │                                                          │
│      ▼                                                          │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ 3. Update Affected Services                              │   │
│  │    - shared/src/shared/schemas.py (Pydantic schemas)    │   │
│  │    - api/app/db/queries/*.py (query changes)            │   │
│  │    - ingestor/app/models.py (validation models)         │   │
│  │    - ingestor/mappings/*.yaml (field mapping)           │   │
│  └─────────────────────────────────────────────────────────┘   │
│      │                                                          │
│      ▼                                                          │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ 4. Test & Deploy                                         │   │
│  │    a. alembic upgrade head                              │   │
│  │    b. Deploy API (read-safe)                            │   │
│  │    c. Deploy Ingestor (write-safe)                      │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Related Documentation

- [API Reference](./API_REFERENCE.md) - Complete endpoint documentation
- [Schema Change Guide](./SCHEMA_CHANGE_GUIDE.md) - How to handle schema changes
- [Ingestor Architecture](./INGESTOR_ARCHITECTURE.md) - Data ingestion pipeline
