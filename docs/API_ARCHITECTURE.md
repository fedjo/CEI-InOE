

## Overview

The API service provides RESTful access to environmental, energy, and dairy production data collected by the CEI-InOE platform. It serves as the primary interface for external applications, dashboards, and integrations.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        CEI-InOE Platform                            │
│                                                                     │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────────┐ │
│  │  External   │    │   TagoIO    │    │   File Sources          │ │
│  │  APIs       │    │   Webhook   │    │   (CSV)                 │ │
│  └──────┬──────┘    └──────┬──────┘    └───────────┬─────────────┘ │
│         │                  │                       │               │
│         └──────────────────┼───────────────────────┘               │
│                            ▼                                       │
│                   ┌─────────────────┐                              │
│                   │    Ingestor     │                              │
│                   │   (Scheduled)   │                              │
│                   └────────┬────────┘                              │
│                            │ writes                                │
│                            ▼                                       │
│  ┌──────────┐      ┌─────────────────┐      ┌──────────────────┐  │
│  │  Flyway  │─────▶│   PostgreSQL    │◀─────│     Grafana      │  │
│  │(migrate) │      │                 │      │   (dashboards)   │  │
│  └──────────┘      │  - Tables       │      └──────────────────┘  │
│                    │  - Views        │                             │
│                    │  - Indexes      │                             │
│                    └────────┬────────┘                             │
│                             │ reads                                │
│                             ▼                                      │
│                    ┌─────────────────┐      ┌──────────────────┐  │
│                    │      API        │◀─────│  React Frontend  │  │
│                    │   (FastAPI)     │      │  (future)        │  │
│                    └─────────────────┘      └──────────────────┘  │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

## Directory Structure

```
api/
├── Dockerfile
├── requirements.txt
├── app/
│   ├── __init__.py
│   ├── main.py                 # FastAPI application entry point
│   ├── config.py               # Configuration from environment
│   │
│   ├── db/
│   │   ├── __init__.py
│   │   ├── connection.py       # Connection pool management
│   │   └── queries/
│   │       ├── __init__.py
│   │       ├── energy.py       # Energy data queries
│   │       ├── environmental.py
│   │       ├── dairy.py
│   │       └── devices.py
│   │
│   ├── schemas/
│   │   ├── __init__.py
│   │   ├── common.py           # Shared schemas (pagination, etc.)
│   │   ├── energy.py           # Energy response models
│   │   ├── environmental.py
│   │   ├── dairy.py
│   │   └── devices.py
│   │
│   └── routers/
│       ├── __init__.py
│       ├── energy.py           # /api/v1/energy/* endpoints
│       ├── environmental.py    # /api/v1/environmental/* endpoints
│       ├── dairy.py            # /api/v1/dairy/* endpoints
│       ├── devices.py          # /api/v1/devices/* endpoints
│       └── health.py           # Health check endpoints
│
└── tests/
    └── (future)
```

## Design Principles

| Principle | Implementation |
|-----------|----------------|
| **Database as Contract** | PostgreSQL schema (via migrations) is the source of truth |
| **Read-Only Access** | API only reads data; ingestor handles writes |
| **Raw SQL Queries** | Direct SQL for performance and clarity |
| **Pydantic Validation** | Response models validate output shape |
| **Stateless** | No session state; scales horizontally |

## Data Flow

### Request Lifecycle

```
HTTP Request
     │
     ▼
┌─────────────────────────────────────────────────────────────┐
│ Router                                                       │
│ - Validate query parameters (Pydantic)                      │
│ - Check authentication (if enabled)                         │
└─────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────┐
│ Query Function                                               │
│ - Build parameterized SQL                                   │
│ - Execute against connection pool                           │
│ - Return raw rows as dictionaries                           │
└─────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────┐
│ PostgreSQL                                                   │
│ - Query tables or views                                     │
│ - Return result set                                         │
└─────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────┐
│ Schema Validation                                            │
│ - Pydantic model validates response structure               │
│ - Serializes to JSON                                        │
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
│  │ 1. Create Migration                                      │   │
│  │    db/migrations/V0XX__description.sql                   │   │
│  └─────────────────────────────────────────────────────────┘   │
│      │                                                          │
│      ▼                                                          │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ 2. Update Affected Services                              │   │
│  │    - ingestor/app/models.py (if ingesting new fields)   │   │
│  │    - api/app/schemas/*.py (if exposing new fields)      │   │
│  │    - api/app/db/queries/*.py (if query changes)         │   │
│  └─────────────────────────────────────────────────────────┘   │
│      │                                                          │
│      ▼                                                          │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ 3. Test & Deploy                                         │   │
│  │    a. Apply migration (Flyway)                          │   │
│  │    b. Deploy API (read-safe)                            │   │
│  │    c. Deploy Ingestor (write-safe)                      │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Related Documentation

- [API Reference](./API_REFERENCE.md) - Complete endpoint documentation
- [Schema Change Guide](./SCHEMA_CHANGE_GUIDE.md) - How to handle schema changes
