
# Schema Change Guide

This guide explains how to handle database schema changes and their impact on the CEI-InOE services.

## Overview

The database schema is the **source of truth** for data structures. Changes flow through:

```
Migration → PostgreSQL → Services (Ingestor + API)
```

## Types of Schema Changes

| Change Type | Complexity | Downtime? |
|-------------|------------|-----------|
| Add nullable column | Low | No |
| Add required column | Medium | No* |
| Rename column | High | No* |
| Drop column | Medium | No* |
| Add table | Medium | No |
| Add view | Low | No |
| Modify view | Low | No |

\* With proper coordination

---

## Example Scenarios

### Scenario 1: Add New Nullable Column

**Requirement:** Add `quality_score` to dairy production.

#### Step 1: Create Migration

```sql
-- db/migrations/V010__dairy_quality_score.sql

ALTER TABLE dairy_production 
ADD COLUMN quality_score DECIMAL(4,2);

COMMENT ON COLUMN dairy_production.quality_score IS 
    'Calculated milk quality score (0-100)';
```

#### Step 2: Update Ingestor (if field comes from source)

```python
# ingestor/app/models.py
class DairyProductionRecord(BaseModel):
    # ...existing fields...
    quality_score: Optional[float] = None  # New field
```

```yaml
# ingestor/app/mappings/dairy_production.yaml
columns:
  # ...existing mappings...
  quality_score:
    source: "Quality Score"
    type: float
    nullable: true
```

#### Step 3: Update API

```python
# api/app/schemas/dairy.py
class DairyProductionRecord(BaseModel):
    # ...existing fields...
    quality_score: float | None = None  # New field
```

```python
# api/app/db/queries/dairy.py
# Add quality_score to SELECT statement
```

#### Step 4: Deploy

```bash
# 1. Apply migration
docker-compose run flyway migrate

# 2. Deploy API (can read existing data, new field will be null)
docker-compose up -d api

# 3. Deploy ingestor (will start writing new field)
docker-compose up -d ingestor
```

**Impact:**
- ✅ API works before ingestor deploys (returns `null` for new field)
- ✅ No downtime required
- ✅ Backward compatible

---

### Scenario 2: Rename Column

**Requirement:** Rename `energy_kwh` to `kwh` in energy tables.

#### Step 1: Migration (Transition Period)

```sql
-- db/migrations/V010__energy_rename_kwh.sql

-- Add new column with trigger to keep both in sync
ALTER TABLE fact_energy_hourly 
ADD COLUMN kwh FLOAT;

UPDATE fact_energy_hourly 
SET kwh = energy_kwh;

-- Create sync trigger
CREATE OR REPLACE FUNCTION sync_energy_kwh()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
        IF NEW.kwh IS NULL AND NEW.energy_kwh IS NOT NULL THEN
            NEW.kwh := NEW.energy_kwh;
        ELSIF NEW.energy_kwh IS NULL AND NEW.kwh IS NOT NULL THEN
            NEW.energy_kwh := NEW.kwh;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_sync_energy_kwh
BEFORE INSERT OR UPDATE ON fact_energy_hourly
FOR EACH ROW EXECUTE FUNCTION sync_energy_kwh();
```

#### Step 2: Update API to use new column name

```python
# api/app/db/queries/energy.py
# Change: energy_kwh as kwh → kwh
```

#### Step 3: Update Ingestor to write to new column

```python
# ingestor/app/...
# Update to write to 'kwh' column
```

#### Step 4: Cleanup migration (after all services updated)

```sql
-- db/migrations/V011__energy_drop_old_kwh.sql

DROP TRIGGER tr_sync_energy_kwh ON fact_energy_hourly;
DROP FUNCTION sync_energy_kwh();
ALTER TABLE fact_energy_hourly DROP COLUMN energy_kwh;
```

**Impact:**
- ⚠️ Requires two-phase deployment
- ✅ Zero downtime with transition period
- ✅ Can rollback during transition

---

### Scenario 3: Add New Table

**Requirement:** Add water consumption tracking.

#### Step 1: Migration

```sql
-- db/migrations/V010__water_consumption.sql

CREATE TABLE water_consumption (
    id SERIAL PRIMARY KEY,
    device_id INTEGER REFERENCES generic_device(id),
    ts TIMESTAMP NOT NULL,
    liters DECIMAL(12,2),
    source_file UUID REFERENCES ingest_file(file_id),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_water_consumption_ts ON water_consumption(ts);
CREATE INDEX idx_water_consumption_device ON water_consumption(device_id);
```

#### Step 2: Add Ingestor Components

```
ingestor/app/
├── models.py                    # Add WaterConsumptionRecord
├── mappings/
│   └── water_consumption.yaml   # New mapping file
└── dao/
    └── (extend data_dao or create water_dao)
```

#### Step 3: Add API Components

```
api/app/
├── schemas/
│   └── water.py                 # New response schemas
├── db/queries/
│   └── water.py                 # New query functions
└── routers/
    └── water.py                 # New router
```

```python
# api/app/main.py
from app.routers import water

app.include_router(
    water.router, 
    prefix="/api/v1/water", 
    tags=["Water"]
)
```

#### Step 4: Deploy

```bash
# 1. Apply migration
docker-compose run flyway migrate

# 2. Deploy API (endpoints ready, no data yet)
docker-compose up -d api

# 3. Deploy ingestor (starts collecting data)
docker-compose up -d ingestor
```

---

### Scenario 4: Add Aggregation View

**Requirement:** Add monthly energy summary view.

#### Step 1: Migration only

```sql
-- db/migrations/V010__energy_monthly_view.sql

CREATE OR REPLACE VIEW v_energy_monthly AS
SELECT 
    DATE_TRUNC('month', ts)::date AS month,
    device_id,
    SUM(energy_kwh) AS total_kwh,
    COUNT(*) AS reading_count
FROM fact_energy_hourly
GROUP BY DATE_TRUNC('month', ts), device_id
ORDER BY month DESC;
```

#### Step 2: Add API endpoint (optional query function)

```python
# api/app/db/queries/energy.py
def get_monthly_summary(limit: int = 12):
    return execute_query(
        "SELECT * FROM v_energy_monthly LIMIT %(limit)s",
        {"limit": limit}
    )
```

**Impact:**
- ✅ No ingestor changes needed
- ✅ Grafana can also query the view
- ✅ Simple deployment

---

## Change Impact Matrix

| Change | Ingestor | API | Grafana |
|--------|----------|-----|---------|
| Add nullable column | Update if ingesting | Update if exposing | May need update |
| Add required column | Must update | Must update | Must update |
| Rename column | Update mappings | Update queries | Update queries |
| Add table | Add model+DAO | Add router+schema | Add dashboard |
| Add view | None | Can query | Can query |
| Drop column | Remove from model | Remove from schema | Remove from queries |

---

## Deployment Order

### Safe Order (No Downtime)

```
1. Apply database migration
   └── Schema changes are live

2. Deploy API service
   └── Reads are safe (new columns return null)
   └── Old endpoints still work

3. Deploy Ingestor service
   └── Writes now include new columns
```

### Why This Order?

- **API first**: Read operations are inherently safe
- **New columns default to null**: Existing API code handles missing data
- **Ingestor last**: Ensures writes are compatible with new schema

---

## Rollback Procedures

### Column Addition Rollback

```sql
-- If something goes wrong after adding a column
ALTER TABLE table_name DROP COLUMN column_name;
```

### Complex Change Rollback

For changes with triggers/functions:

```sql
-- Store rollback script
-- db/migrations/V010__change_ROLLBACK.sql (not applied automatically)
DROP TRIGGER IF EXISTS trigger_name ON table_name;
DROP FUNCTION IF EXISTS function_name();
ALTER TABLE table_name DROP COLUMN IF EXISTS new_column;
```

---

## Pre-Change Checklist

- [ ] Document the change requirement
- [ ] Identify affected services (Ingestor, API, Grafana)
- [ ] Write migration SQL
- [ ] Test migration on local database
- [ ] Update Ingestor models/mappings (if applicable)
- [ ] Update API schemas/queries (if applicable)
- [ ] Run tests for both services
- [ ] Plan deployment order
- [ ] Prepare rollback procedure

## Post-Change Checklist

- [ ] Migration applied successfully
- [ ] API health check passes
- [ ] API endpoints return expected data
- [ ] Ingestor starts without errors
- [ ] New data is being ingested correctly
- [ ] Grafana dashboards work (if affected)
- [ ] Monitor for errors in logs
