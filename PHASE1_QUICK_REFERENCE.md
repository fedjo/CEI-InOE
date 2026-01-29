# Phase 1 Refactoring - Quick Reference

## What Changed?

### The Big Picture
✅ **Unified CSV & API ingestion** - Same pipeline for both sources  
✅ **Explicit validation** - Know exactly what failed and why  
✅ **Staging area** - Quality gates before warehouse  
✅ **Full observability** - Track every pipeline stage  
✅ **Source tracking** - Know if data came from CSV or API  
✅ **Backward compatible** - Legacy code still works  

---

## Key Components

### 1. New Modules
```
ingestor/app/
├── validation.py     → Schema, type, business rule validation
├── transformation.py → Unified data transformation
├── staging.py        → Staging operations & conflict resolution
└── pipeline.py       → Orchestrate Extract→Validate→Transform→Load
```

### 2. New Database Tables
```sql
pipeline_execution      → Track each stage execution
data_quality_checks     → Validation results & metrics
staging_*               → Temporary data before final load
device                  → Unified device registry (CSV & API)
```

### 3. Enhanced Existing Tables
All data tables now have:
- `source_type` - 'csv' or 'api'
- `source_file` - UUID link to ingest_file
- `source_api_endpoint` - API URL if from API
- `source_device_id` - Device identifier
- `ingestion_method` - 'batch' or 'streaming'
- `ingested_at` - Timestamp

---

## How to Use

### Automatic (File Watcher)
Just drop files in `/data/incoming/` - the new pipeline runs automatically!

```bash
# The ingestor now uses new pipeline by default
docker-compose up ingestor

# Check logs
docker-compose logs -f ingestor | grep PIPELINE
```

### Programmatic (CSV)
```python
from app.pipeline import run_csv_pipeline
import yaml

with open('mappings/environmental_metrics.yaml') as f:
    mapping = yaml.safe_load(f)

metrics = run_csv_pipeline(
    file_path='/path/to/data.csv',
    connection=db_connection,
    mapping=mapping,
    device_id='device_001'
)

print(f"✓ {metrics.load_records} records loaded")
print(f"✗ {metrics.invalid_records} records failed validation")
```

### Programmatic (API)
```python
from app.pipeline import run_api_pipeline

# Get data from API
api_data = fetch_from_api()

# Run through same pipeline
metrics = run_api_pipeline(
    api_records=api_data,
    connection=db_connection,
    mapping=mapping,
    api_endpoint='https://api.example.com/v1/data',
    device_id='api_device_123'
)
```

---

## Monitoring & Debugging

### Check Pipeline Health
```sql
-- Overall pipeline status
SELECT * FROM v_pipeline_health
WHERE last_execution >= NOW() - INTERVAL '24 hours';

-- Failed stages
SELECT * FROM pipeline_execution
WHERE status = 'failed'
ORDER BY started_at DESC
LIMIT 10;
```

### Check Data Quality
```sql
-- Quality summary by dataset
SELECT * FROM v_data_quality_summary;

-- Detailed validation errors
SELECT 
    dataset,
    check_type,
    failed_count,
    sample_failures
FROM data_quality_checks
WHERE passed = FALSE
ORDER BY checked_at DESC;
```

### Check Staging Data
```sql
-- Invalid records with errors
SELECT 
    row_number,
    raw_data,
    validation_errors
FROM staging_environmental_metrics
WHERE is_valid = FALSE
LIMIT 10;

-- Statistics
SELECT 
    COUNT(*) as total,
    SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) as valid,
    SUM(CASE WHEN NOT is_valid THEN 1 ELSE 0 END) as invalid
FROM staging_environmental_metrics
WHERE created_at >= NOW() - INTERVAL '24 hours';
```

### Check Source Distribution
```sql
-- Compare CSV vs API sources
SELECT * FROM v_source_comparison
WHERE dataset = 'environmental_metrics';

-- Devices by source type
SELECT 
    device_type,
    primary_source,
    status,
    COUNT(*) as count
FROM device
GROUP BY device_type, primary_source, status;
```

---

## YAML Configuration

### Add Validation Rules
```yaml
validation_rules:
  schema:
    required:
      - timestamp
      - temperature
  
  constraints:
    temperature:
      min: -50
      max: 60
    humidity:
      min: 0
      max: 100
```

### Configure Conflict Resolution
```yaml
conflict_resolution:
  strategy: update    # or 'ignore', 'fail', 'append'
  on_columns:
    - timestamp
  update_columns:
    - temperature
    - humidity
```

---

## Common Scenarios

### Scenario 1: File Has Validation Errors
**What happens?**
1. File is extracted to staging
2. Invalid rows flagged in staging table
3. Valid rows loaded to final table
4. Errors logged to data_quality_checks
5. File moved to processed/ (not rejected!)

**How to fix?**
```sql
-- Find what failed
SELECT validation_errors FROM staging_environmental_metrics
WHERE is_valid = FALSE;

-- Fix and reprocess if needed
-- (Manual SQL or re-drop file)
```

### Scenario 2: Duplicate File
**What happens?**
1. SHA256 check detects duplicate
2. File skipped (logged)
3. Moved to processed/
4. No duplicate data in database

### Scenario 3: Source Comparison
**Query CSV vs API data quality:**
```sql
SELECT 
    source_type,
    COUNT(*) as records,
    COUNT(CASE WHEN temperature IS NULL THEN 1 END) as null_temps,
    AVG(temperature) as avg_temp,
    STDDEV(temperature) as stddev_temp
FROM environmental_metrics
WHERE timestamp >= NOW() - INTERVAL '7 days'
GROUP BY source_type;
```

### Scenario 4: Pipeline Performance
**Check slow stages:**
```sql
SELECT 
    stage,
    AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) as avg_duration,
    MAX(EXTRACT(EPOCH FROM (completed_at - started_at))) as max_duration
FROM pipeline_execution
WHERE started_at >= NOW() - INTERVAL '24 hours'
AND status = 'success'
GROUP BY stage
ORDER BY avg_duration DESC;
```

---

## Rollback Plan

If issues arise, disable new pipeline:

### Method 1: Code Change
```python
# In main.py, force legacy mode
PIPELINE_ENABLED = False
```

### Method 2: Environment Variable
```bash
export USE_LEGACY_PIPELINE=true
```

### Method 3: Rename Modules
```bash
cd ingestor/app
mv pipeline.py pipeline.py.disabled
# Restart - will auto-fallback to legacy
```

Legacy functions are preserved and still work!

---

## Performance Tips

### Staging Cleanup
```sql
-- Clean old staging data (keeps last 7 days)
SELECT * FROM cleanup_staging_data(7);
```

### Batch Size
For large files, consider splitting:
```python
# Process in chunks
for chunk in pd.read_csv(file, chunksize=1000):
    # Process chunk through pipeline
```

### Indexes
Already created in V007:
- Pipeline execution by file_id
- Quality checks by dataset
- Staging by file_id and validity

---

## Quick Commands

### Start Ingestor
```bash
docker-compose up -d ingestor
docker-compose logs -f ingestor
```

### Apply Migration
```bash
docker-compose exec postgres psql -U user -d dbname -f /migrations/V007__pipeline_metadata_and_staging.sql
```

### Check Pipeline Mode
```bash
docker-compose logs ingestor | grep "Pipeline Mode"
# Should see: "Pipeline Mode: NEW (Phase 1)"
```

### Test File
```bash
# Drop test file
cp test_data.csv data/incoming/
# Watch logs
docker-compose logs -f ingestor
# Check database
docker-compose exec postgres psql -U user -d dbname -c "SELECT * FROM pipeline_execution ORDER BY started_at DESC LIMIT 5;"
```

---

## Support Matrix

| Feature | Legacy | New Pipeline |
|---------|--------|--------------|
| CSV ingestion | ✅ | ✅ |
| API ingestion | ⚠️ Manual | ✅ Automatic |
| Validation | ❌ Silent | ✅ Explicit |
| Staging | ❌ | ✅ |
| Source tracking | ❌ | ✅ |
| Quality metrics | ❌ | ✅ |
| Observability | ⚠️ Basic | ✅ Full |
| Conflict resolution | ⚠️ Hardcoded | ✅ Configurable |
| Error recovery | ❌ | ✅ |

---

## Next Steps

1. ✅ Apply V007 migration
2. ✅ Deploy new code
3. ⏳ Test with sample files
4. ⏳ Monitor pipeline_execution
5. ⏳ Review data_quality_checks
6. ⏳ Compare source types
7. ⏳ Optimize as needed

---

## Questions?

Check:
1. [PHASE1_SUMMARY.md](PHASE1_SUMMARY.md) - Detailed documentation
2. Pipeline execution logs in database
3. Data quality checks table
4. Staging tables for failed records
5. Legacy functions still available as fallback
