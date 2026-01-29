-- V007: Pipeline Metadata, Staging Tables, and Source Tracking
-- Phase 1 Refactoring: Add observability and staging infrastructure

-- ============================================================================
-- Pipeline Execution Tracking
-- ============================================================================

CREATE TABLE IF NOT EXISTS pipeline_execution (
    id SERIAL PRIMARY KEY,
    file_id UUID REFERENCES ingest_file(file_id),
    pipeline_name TEXT NOT NULL,
    stage TEXT NOT NULL,  -- 'extract', 'validate', 'transform', 'load'
    started_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL,  -- 'running', 'success', 'failed', 'skipped'
    records_in INT DEFAULT 0,
    records_out INT DEFAULT 0,
    error_message TEXT,
    execution_metadata JSONB,  -- Additional context (config, params, etc.)
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_pipeline_execution_file ON pipeline_execution(file_id);
CREATE INDEX idx_pipeline_execution_stage ON pipeline_execution(stage, status);
CREATE INDEX idx_pipeline_execution_started ON pipeline_execution(started_at DESC);

COMMENT ON TABLE pipeline_execution IS 'Tracks each stage of pipeline execution for observability';

-- ============================================================================
-- Data Quality Checks
-- ============================================================================

CREATE TABLE IF NOT EXISTS data_quality_checks (
    id SERIAL PRIMARY KEY,
    file_id UUID REFERENCES ingest_file(file_id),
    dataset TEXT NOT NULL,
    check_type TEXT NOT NULL,  -- 'schema', 'type', 'range', 'uniqueness', 'completeness'
    check_name TEXT NOT NULL,
    passed BOOLEAN NOT NULL,
    failed_count INT DEFAULT 0,
    total_count INT DEFAULT 0,
    failure_rate NUMERIC(5,2),  -- Percentage
    sample_failures JSONB,  -- Examples of failures for debugging
    checked_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_quality_checks_file ON data_quality_checks(file_id);
CREATE INDEX idx_quality_checks_dataset ON data_quality_checks(dataset, check_type);
CREATE INDEX idx_quality_checks_passed ON data_quality_checks(passed, checked_at DESC);

COMMENT ON TABLE data_quality_checks IS 'Records data quality validation results';

-- ============================================================================
-- Add Source Metadata to Existing Tables
-- ============================================================================

-- Environmental Metrics
ALTER TABLE environmental_metrics 
    ADD COLUMN IF NOT EXISTS source_type TEXT DEFAULT 'csv',
    ADD COLUMN IF NOT EXISTS source_file UUID REFERENCES ingest_file(file_id),
    ADD COLUMN IF NOT EXISTS source_api_endpoint TEXT,
    ADD COLUMN IF NOT EXISTS source_device_id TEXT,
    ADD COLUMN IF NOT EXISTS ingestion_method TEXT DEFAULT 'batch',
    ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ DEFAULT NOW();

CREATE INDEX IF NOT EXISTS idx_env_metrics_source ON environmental_metrics(source_type, source_device_id);
CREATE INDEX IF NOT EXISTS idx_env_metrics_file ON environmental_metrics(source_file);

-- Energy Hourly
ALTER TABLE fact_energy_hourly
    ADD COLUMN IF NOT EXISTS source_type TEXT DEFAULT 'csv',
    ADD COLUMN IF NOT EXISTS source_file UUID REFERENCES ingest_file(file_id),
    ADD COLUMN IF NOT EXISTS source_api_endpoint TEXT,
    ADD COLUMN IF NOT EXISTS source_device_id TEXT,
    ADD COLUMN IF NOT EXISTS ingestion_method TEXT DEFAULT 'batch',
    ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ DEFAULT NOW();

CREATE INDEX IF NOT EXISTS idx_energy_hourly_source ON fact_energy_hourly(source_type, source_device_id);
CREATE INDEX IF NOT EXISTS idx_energy_hourly_file ON fact_energy_hourly(source_file);

-- Energy Daily
ALTER TABLE fact_energy_daily
    ADD COLUMN IF NOT EXISTS source_type TEXT DEFAULT 'csv',
    ADD COLUMN IF NOT EXISTS source_file UUID REFERENCES ingest_file(file_id),
    ADD COLUMN IF NOT EXISTS source_api_endpoint TEXT,
    ADD COLUMN IF NOT EXISTS source_device_id TEXT,
    ADD COLUMN IF NOT EXISTS ingestion_method TEXT DEFAULT 'batch',
    ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ DEFAULT NOW();

CREATE INDEX IF NOT EXISTS idx_energy_daily_source ON fact_energy_daily(source_type, source_device_id);
CREATE INDEX IF NOT EXISTS idx_energy_daily_file ON fact_energy_daily(source_file);

-- Dairy Production
ALTER TABLE dairy_production
    ADD COLUMN IF NOT EXISTS source_type TEXT DEFAULT 'csv',
    ADD COLUMN IF NOT EXISTS source_file UUID REFERENCES ingest_file(file_id),
    ADD COLUMN IF NOT EXISTS source_api_endpoint TEXT,
    ADD COLUMN IF NOT EXISTS source_device_id TEXT,
    ADD COLUMN IF NOT EXISTS ingestion_method TEXT DEFAULT 'batch',
    ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ DEFAULT NOW();

CREATE INDEX IF NOT EXISTS idx_dairy_prod_source ON dairy_production(source_type, source_device_id);
CREATE INDEX IF NOT EXISTS idx_dairy_prod_file ON dairy_production(source_file);

-- ============================================================================
-- Update Ingest File Table
-- ============================================================================

ALTER TABLE ingest_file 
    ADD COLUMN IF NOT EXISTS execution_time_ms INT,
    ADD COLUMN IF NOT EXISTS validation_status TEXT,  -- 'passed', 'failed', 'partial'
    ADD COLUMN IF NOT EXISTS quality_score NUMERIC(5,2),  -- Overall quality percentage
    ADD COLUMN IF NOT EXISTS pipeline_version TEXT DEFAULT '1.0';

CREATE INDEX IF NOT EXISTS idx_ingest_file_validation ON ingest_file(validation_status, ingested_at DESC);

-- ============================================================================
-- Staging Tables
-- ============================================================================

-- Staging: Environmental Metrics
CREATE TABLE IF NOT EXISTS staging_environmental_metrics (
    staging_id SERIAL PRIMARY KEY,
    file_id UUID REFERENCES ingest_file(file_id),
    row_number INT NOT NULL,
    raw_data JSONB NOT NULL,  -- Original CSV/API data
    transformed_data JSONB,  -- After transformation
    validation_errors JSONB,  -- List of validation errors
    is_valid BOOLEAN DEFAULT FALSE,
    loaded_to_final BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_staging_env_file ON staging_environmental_metrics(file_id);
CREATE INDEX idx_staging_env_valid ON staging_environmental_metrics(is_valid, loaded_to_final);

-- Staging: Energy Hourly
CREATE TABLE IF NOT EXISTS staging_energy_hourly (
    staging_id SERIAL PRIMARY KEY,
    file_id UUID REFERENCES ingest_file(file_id),
    row_number INT NOT NULL,
    raw_data JSONB NOT NULL,
    transformed_data JSONB,
    validation_errors JSONB,
    is_valid BOOLEAN DEFAULT FALSE,
    loaded_to_final BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_staging_energy_hourly_file ON staging_energy_hourly(file_id);
CREATE INDEX idx_staging_energy_hourly_valid ON staging_energy_hourly(is_valid, loaded_to_final);

-- Staging: Energy Daily
CREATE TABLE IF NOT EXISTS staging_energy_daily (
    staging_id SERIAL PRIMARY KEY,
    file_id UUID REFERENCES ingest_file(file_id),
    row_number INT NOT NULL,
    raw_data JSONB NOT NULL,
    transformed_data JSONB,
    validation_errors JSONB,
    is_valid BOOLEAN DEFAULT FALSE,
    loaded_to_final BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_staging_energy_daily_file ON staging_energy_daily(file_id);
CREATE INDEX idx_staging_energy_daily_valid ON staging_energy_daily(is_valid, loaded_to_final);

-- Staging: Dairy Production
CREATE TABLE IF NOT EXISTS staging_dairy_production (
    staging_id SERIAL PRIMARY KEY,
    file_id UUID REFERENCES ingest_file(file_id),
    row_number INT NOT NULL,
    raw_data JSONB NOT NULL,
    transformed_data JSONB,
    validation_errors JSONB,
    is_valid BOOLEAN DEFAULT FALSE,
    loaded_to_final BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_staging_dairy_file ON staging_dairy_production(file_id);
CREATE INDEX idx_staging_dairy_valid ON staging_dairy_production(is_valid, loaded_to_final);

-- ============================================================================
-- Enhanced Device Table (Unified for CSV & API sources)
-- ============================================================================

CREATE TABLE IF NOT EXISTS device (
    id SERIAL PRIMARY KEY,
    device_id TEXT UNIQUE NOT NULL,
    device_type TEXT NOT NULL,  -- 'energy_meter', 'weather_station', 'air_quality'
    
    -- Source configuration
    primary_source TEXT NOT NULL,  -- 'csv', 'api', 'hybrid'
    api_endpoint TEXT,
    api_credentials_id UUID,
    
    -- Device metadata
    location TEXT,
    farm_id TEXT,
    manufacturer TEXT,
    model TEXT,
    installation_date DATE,
    status TEXT DEFAULT 'active',  -- 'active', 'inactive', 'maintenance'
    
    -- API-specific (NULL for CSV-only devices)
    api_polling_interval INT,  -- Minutes
    last_api_poll TIMESTAMPTZ,
    api_poll_failures INT DEFAULT 0,
    
    -- CSV-specific (NULL for API-only devices)
    expected_csv_pattern TEXT,  -- Filename regex
    last_csv_upload TIMESTAMPTZ,
    
    -- Metadata
    configuration JSONB,  -- Device-specific config
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- CREATE INDEX idx_device_type_source ON device(device_type, primary_source);
-- CREATE INDEX idx_device_status ON device(status, primary_source);

-- COMMENT ON TABLE device IS 'Unified device registry supporting both CSV and API data sources';

-- ============================================================================
-- Views for Analytics
-- ============================================================================

-- Pipeline Health Dashboard
CREATE OR REPLACE VIEW v_pipeline_health AS
SELECT 
    pe.pipeline_name,
    pe.stage,
    pe.status,
    COUNT(*) as execution_count,
    AVG(EXTRACT(EPOCH FROM (pe.completed_at - pe.started_at))) as avg_duration_seconds,
    SUM(pe.records_in) as total_records_in,
    SUM(pe.records_out) as total_records_out,
    SUM(pe.records_in - pe.records_out) as total_records_lost,
    MAX(pe.completed_at) as last_execution
FROM pipeline_execution pe
WHERE pe.started_at >= NOW() - INTERVAL '7 days'
GROUP BY pe.pipeline_name, pe.stage, pe.status
ORDER BY pe.pipeline_name, pe.stage;

-- Data Quality Dashboard
CREATE OR REPLACE VIEW v_data_quality_summary AS
SELECT 
    dqc.dataset,
    dqc.check_type,
    COUNT(*) as total_checks,
    SUM(CASE WHEN dqc.passed THEN 1 ELSE 0 END) as passed_checks,
    SUM(CASE WHEN NOT dqc.passed THEN 1 ELSE 0 END) as failed_checks,
    ROUND(AVG(CASE WHEN dqc.passed THEN 100.0 ELSE 0.0 END), 2) as pass_rate,
    MAX(dqc.checked_at) as last_check
FROM data_quality_checks dqc
WHERE dqc.checked_at >= NOW() - INTERVAL '7 days'
GROUP BY dqc.dataset, dqc.check_type
ORDER BY dqc.dataset, dqc.check_type;

-- Source Comparison
CREATE OR REPLACE VIEW v_source_comparison AS
SELECT
    'environmental_metrics' as dataset,
    source_type,
    COUNT(*) as record_count,
    MIN(timestamp) as earliest_record,
    MAX(timestamp) as latest_record,
    COUNT(DISTINCT source_device_id) as unique_devices
FROM environmental_metrics
WHERE timestamp >= NOW() - INTERVAL '30 days'
GROUP BY source_type

UNION ALL

SELECT 
    'fact_energy_hourly' as dataset,
    source_type,
    COUNT(*) as record_count,
    MIN(ts) as earliest_record,
    MAX(ts) as latest_record,
    COUNT(DISTINCT source_device_id) as unique_devices
FROM fact_energy_hourly
WHERE ts >= NOW() - INTERVAL '30 days'
GROUP BY source_type

UNION ALL

SELECT 
    'fact_energy_daily' as dataset,
    source_type,
    COUNT(*) as record_count,
    MIN(ts) as earliest_record,
    MAX(ts) as latest_record,
    COUNT(DISTINCT source_device_id) as unique_devices
FROM fact_energy_daily
WHERE ts >= NOW() - INTERVAL '30 days'
GROUP BY source_type

UNION ALL

SELECT 
    'dairy_production' as dataset,
    source_type,
    COUNT(*) as record_count,
    MIN(production_date) as earliest_record,
    MAX(production_date) as latest_record,
    COUNT(DISTINCT source_device_id) as unique_devices
FROM dairy_production
WHERE production_date >= NOW() - INTERVAL '30 days'
GROUP BY source_type;

-- ============================================================================
-- Utility Functions
-- ============================================================================

-- Function to clean old staging data (retention policy)
CREATE OR REPLACE FUNCTION cleanup_staging_data(retention_days INT DEFAULT 7)
RETURNS TABLE(table_name TEXT, deleted_count BIGINT) AS $$
BEGIN
    DELETE FROM staging_environmental_metrics WHERE created_at < NOW() - (retention_days || ' days')::INTERVAL AND loaded_to_final = TRUE;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    table_name := 'staging_environmental_metrics';
    RETURN NEXT;
    
    DELETE FROM staging_energy_hourly WHERE created_at < NOW() - (retention_days || ' days')::INTERVAL AND loaded_to_final = TRUE;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    table_name := 'staging_energy_hourly';
    RETURN NEXT;
    
    DELETE FROM staging_energy_daily WHERE created_at < NOW() - (retention_days || ' days')::INTERVAL AND loaded_to_final = TRUE;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    table_name := 'staging_energy_daily';
    RETURN NEXT;
    
    DELETE FROM staging_dairy_production WHERE created_at < NOW() - (retention_days || ' days')::INTERVAL AND loaded_to_final = TRUE;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    table_name := 'staging_dairy_production';
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cleanup_staging_data IS 'Cleanup old staging records that have been successfully loaded';
