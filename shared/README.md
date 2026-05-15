# CEI-InOE Shared Models

Shared SQLAlchemy models used by both `api/` and `ingestor/` services.

## Installation

From the project root:

```bash
# For development (editable install)
pip install -e ./shared

# Or add to requirements.txt
-e ./shared
```

## Usage

### API (ORM style)
```python
from sqlalchemy.orm import Session
from shared.models import Datasource

def get_datasource(db: Session, datasource_id: int):
    return db.query(Datasource).filter(Datasource.id == datasource_id).first()
```

### Ingestor (Core style)
```python
from sqlalchemy import select
from shared.models import Datasource

# Access the underlying Table object
table = Datasource.__table__

stmt = select(table.c.id).where(table.c.external_id == "some-id")
result = conn.execute(stmt)
```

## Models

- **Datasource**: Unified source registry (devices, files, APIs)
- **IngestBatch**: Ingestion batch tracking (replaces ingest_file)
- **FactEnergyHourly**: Hourly energy readings
- **FactEnergyDaily**: Daily energy aggregates
- **EnvironmentalMetrics**: Weather/environmental data
- **DairyProduction**: Dairy production metrics
- **PipelineExecution**: Pipeline stage tracking
- **DataQualityCheck**: Data quality validation results
- **ApiFetchCursor**: API incremental fetch tracking
- **Staging tables**: Temporary staging for each data type

## Migrations

Migrations are managed with Alembic from the project root:

```bash
cd /path/to/CEI-InOE

# Generate a new migration
alembic revision --autogenerate -m "description"

# Apply migrations
alembic upgrade head

# Rollback
alembic downgrade -1
```
