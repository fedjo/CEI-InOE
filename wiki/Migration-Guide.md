
# Migration Guide: Flyway → Alembic + SQLAlchemy

This guide documents the migration from Flyway SQL migrations to Alembic with SQLAlchemy models.

## Overview of Changes

### Schema Changes
| Old Name | New Name | Notes |
|----------|----------|-------|
| `device` | `datasource` | Generic term for all data sources |
| `generic_device` | (merged into `datasource`) | Unified model |
| `ingest_file` | `ingest_batch` | Better reflects batch processing |
| `file_id` (FK) | `batch_id` | Foreign key renamed |

### Architecture Changes
- **API Service**: Now uses SQLAlchemy ORM for clean, type-safe queries
- **Ingestor Service**: Uses SQLAlchemy Core for high-performance bulk operations
- **Shared Package**: Single source of truth for models, schemas, and database utilities

---

## Step-by-Step Migration

### Step 1: Backup Your Data

```bash
# Dump existing data
pg_dump $DATABASE_URL > backup_$(date +%Y%m%d).sql

# Or export specific tables
pg_dump $DATABASE_URL -t fact_energy_hourly -t fact_energy_daily > energy_backup.sql
```

### Step 2: Set Up Environment

```bash
# From project root
cd /path/to/CEI-InOE

# Create/activate virtual environment
python -m venv venv
source venv/bin/activate

# Install shared package (editable mode)
pip install -e ./shared

# Install migration dependencies
pip install alembic psycopg2-binary
```

### Step 3: Review Migration

Check the initial migration at `alembic/versions/20260226_0001_001_initial_*.py`:

```bash
# Show SQL that would be generated
alembic upgrade head --sql
```

### Step 4: Apply Migration

**Option A: Fresh Database (Recommended for dev)**
```bash
# Drop existing schema and apply fresh
psql $DATABASE_URL -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
alembic upgrade head
```

**Option B: Side-by-Side (For production planning)**
```bash
# Create new tables alongside old ones, then migrate data
alembic upgrade head
# Data migration scripts would go here
```

### Step 5: Seed Data

```bash
# Load datasource definitions
psql $DATABASE_URL -f db/seeds/datasources.sql
```

### Step 6: Update Services

#### API Service

Replace the router imports in `api/app/main.py`:

```python
# Old imports (remove)
from app.routers import devices, energy, environmental, dairy

# New imports (add)
from app.routers import datasources, energy_orm, environmental_orm, dairy_orm, batches
```

Update router registration:
```python
# Old (remove)
app.include_router(devices.router)
app.include_router(energy.router)

# New (add)
app.include_router(datasources.router)
app.include_router(energy_orm.router)
app.include_router(environmental_orm.router)
app.include_router(dairy_orm.router)
app.include_router(batches.router)
```

Or simply use the new `main_orm.py`:
```bash
# Replace main.py with the ORM version
cp api/app/main_orm.py api/app/main.py
```

#### Ingestor Service

Update DAO imports in ingestor files:

```python
# Old import
from app.dao.factory import DAOFactory

# New import  
from app.core_dao.factory import CoreDAOFactory
```

Update DAO usage:
```python
# Old
factory = DAOFactory(conn)
device_dao = factory.get_device_dao()
file_dao = factory.get_ingest_file_dao()
device_id = device_dao.get_or_create_device(external_id, source_type)
file_id = file_dao.create_or_get_file(filename, device_id)

# New
factory = CoreDAOFactory(conn)
datasource_dao = factory.get_datasource_dao()
batch_dao = factory.get_batch_dao()
datasource_id = datasource_dao.get_or_create(external_id, source_type)
batch_id = batch_dao.create_or_get_batch(filename, datasource_id)
```

### Step 7: Update Pipeline Mappings

Update YAML mapping files to use new column names:

```yaml
# Old mapping
foreign_keys:
  file_id: "{{ file_id }}"

# New mapping
foreign_keys:
  batch_id: "{{ batch_id }}"
```

### Step 8: Docker Deployment

Use the new docker-compose configuration:

```bash
# Backup old compose file
mv docker-compose.yaml docker-compose.old.yaml
mv docker-compose.new.yaml docker-compose.yaml

# Run with migrations
docker-compose up -d
```

The new compose file includes an `alembic-migrations` service that runs automatically on startup.

---

## Verification Checklist

- [ ] All tables created: `datasource`, `ingest_batch`, `fact_energy_*`, etc.
- [ ] Indexes created on timestamp and foreign key columns
- [ ] Foreign key constraints active
- [ ] API health endpoint returns OK: `curl http://localhost:8000/health`
- [ ] Ingestor can process test file
- [ ] Grafana dashboards updated to use new table names

---

## Rollback Plan

If issues occur:

```bash
# Revert to old schema
alembic downgrade base

# Or restore from backup
psql $DATABASE_URL < backup_YYYYMMDD.sql
```

---

## File Reference

| File | Purpose |
|------|---------|
| `shared/src/shared/models.py` | SQLAlchemy model definitions |
| `shared/src/shared/schemas.py` | Pydantic schemas for API |
| `shared/src/shared/database.py` | Database connection utilities |
| `alembic/versions/*.py` | Migration files |
| `api/app/routers/*_orm.py` | New ORM-based API routers |
| `api/app/db/queries/*.py` | ORM query functions |
| `ingestor/app/core_dao/*.py` | SQLAlchemy Core DAOs |

---

## Common Issues

### "Import 'shared' could not be resolved"
Install the shared package: `pip install -e ./shared`

### "relation 'datasource' does not exist"
Run migrations: `alembic upgrade head`

### "column 'file_id' does not exist"
Update code to use `batch_id` instead of `file_id`

### Foreign key violations during data migration
Ensure `datasource` records exist before inserting `ingest_batch` records.
