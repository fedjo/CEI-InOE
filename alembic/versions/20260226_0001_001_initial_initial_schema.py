"""Initial schema with datasource and ingest_batch

Revision ID: 001_initial
Revises: 
Create Date: 2026-02-26

Creates the complete CEI-InOE schema with:
- datasource (replaces device/generic_device)
- ingest_batch (replaces ingest_file)
- fact tables (energy_hourly, energy_daily, environmental_metrics, dairy_production)
- pipeline tracking (pipeline_execution, data_quality_check)
- api_fetch_cursor
- staging tables
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '001_initial'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # === Datasource (replaces device/generic_device) ===
    op.create_table('datasource',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('external_id', sa.String(length=64), nullable=False, comment='External identifier (API ID, serial number, etc.)'),
        sa.Column('source_category', sa.String(length=32), nullable=False, comment='Category: device, file, api, manual'),
        sa.Column('data_type', sa.String(length=32), nullable=False, comment='Type: energy, weather, dairy, pv'),
        sa.Column('name', sa.String(length=255), nullable=True),
        sa.Column('alias', sa.String(length=255), nullable=True),
        sa.Column('client', sa.String(length=255), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('status', sa.String(length=32), nullable=False, comment='Status: active, inactive, deprecated'),
        sa.Column('timezone', sa.String(length=64), nullable=False),
        sa.Column('metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=False, comment='Type-specific configuration and metadata'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('external_id')
    )
    op.create_index('idx_datasource_category', 'datasource', ['source_category'], unique=False)
    op.create_index('idx_datasource_client', 'datasource', ['client'], unique=False)
    op.create_index('idx_datasource_data_type', 'datasource', ['data_type'], unique=False)
    op.create_index('idx_datasource_external_id', 'datasource', ['external_id'], unique=False)
    op.create_index('idx_datasource_metadata', 'datasource', ['metadata'], unique=False, postgresql_using='gin')
    op.create_index('idx_datasource_status', 'datasource', ['status'], unique=False)

    # === IngestBatch (replaces ingest_file) ===
    op.create_table('ingest_batch',
        sa.Column('batch_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('source_type', sa.String(length=32), nullable=False, comment='Source type: file, api, stream, manual'),
        sa.Column('source_name', sa.Text(), nullable=False, comment='Filename, API endpoint, or description'),
        sa.Column('datasource_id', sa.Integer(), nullable=True),
        sa.Column('file_sha256', sa.String(length=64), nullable=True, comment='File hash for deduplication'),
        sa.Column('granularity', sa.String(length=32), nullable=True, comment='Data granularity: hourly, daily'),
        sa.Column('date_range_start', sa.Date(), nullable=True),
        sa.Column('date_range_end', sa.Date(), nullable=True),
        sa.Column('status', sa.String(length=32), nullable=False, comment='Status: pending, processing, completed, failed'),
        sa.Column('records_loaded', sa.Integer(), nullable=False),
        sa.Column('records_failed', sa.Integer(), nullable=False),
        sa.Column('execution_time_ms', sa.Integer(), nullable=True),
        sa.Column('validation_status', sa.String(length=32), nullable=True, comment='Validation: passed, failed, partial'),
        sa.Column('quality_score', sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column('pipeline_version', sa.String(length=32), nullable=False),
        sa.Column('started_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['datasource_id'], ['datasource.id'], ),
        sa.PrimaryKeyConstraint('batch_id'),
        sa.UniqueConstraint('file_sha256')
    )
    op.create_index('idx_ingest_batch_datasource', 'ingest_batch', ['datasource_id'], unique=False)
    op.create_index('idx_ingest_batch_source_type', 'ingest_batch', ['source_type'], unique=False)
    op.create_index('idx_ingest_batch_status', 'ingest_batch', ['status'], unique=False)
    op.create_index('idx_ingest_batch_validation', 'ingest_batch', ['validation_status', 'started_at'], unique=False)

    # === API Fetch Cursor ===
    op.create_table('api_fetch_cursor',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('connector_id', sa.Text(), nullable=False),
        sa.Column('endpoint_id', sa.Text(), nullable=False),
        sa.Column('device_id', sa.Text(), nullable=False),
        sa.Column('last_fetch_timestamp', sa.DateTime(timezone=True), nullable=False),
        sa.Column('last_fetch_success', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('fetch_count', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('connector_id', 'endpoint_id', 'device_id', name='uq_api_cursor_connector_endpoint_device')
    )
    op.create_index('idx_api_cursor_lookup', 'api_fetch_cursor', ['connector_id', 'endpoint_id', 'device_id'], unique=False)

    # === Pipeline Execution ===
    op.create_table('pipeline_execution',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('batch_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('pipeline_name', sa.Text(), nullable=False),
        sa.Column('stage', sa.Text(), nullable=False, comment='Stage: extract, validate, transform, load'),
        sa.Column('started_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('status', sa.Text(), nullable=False, comment='Status: running, success, failed, skipped'),
        sa.Column('records_in', sa.Integer(), nullable=False),
        sa.Column('records_out', sa.Integer(), nullable=False),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('execution_metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['batch_id'], ['ingest_batch.batch_id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_pipeline_exec_batch', 'pipeline_execution', ['batch_id'], unique=False)
    op.create_index('idx_pipeline_exec_stage', 'pipeline_execution', ['stage', 'status'], unique=False)
    op.create_index('idx_pipeline_exec_started', 'pipeline_execution', ['started_at'], unique=False)

    # === Data Quality Check ===
    op.create_table('data_quality_check',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('batch_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('dataset', sa.Text(), nullable=False),
        sa.Column('check_type', sa.Text(), nullable=False, comment='Type: schema, type, range, uniqueness, completeness'),
        sa.Column('check_name', sa.Text(), nullable=False),
        sa.Column('passed', sa.Boolean(), nullable=False),
        sa.Column('failed_count', sa.Integer(), nullable=False),
        sa.Column('total_count', sa.Integer(), nullable=False),
        sa.Column('failure_rate', sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column('sample_failures', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('checked_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['batch_id'], ['ingest_batch.batch_id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_quality_check_batch', 'data_quality_check', ['batch_id'], unique=False)
    op.create_index('idx_quality_check_dataset', 'data_quality_check', ['dataset', 'check_type'], unique=False)
    op.create_index('idx_quality_check_passed', 'data_quality_check', ['passed', 'checked_at'], unique=False)

    # === Fact Energy Hourly ===
    op.create_table('fact_energy_hourly',
        sa.Column('energy_id', sa.Integer(), nullable=False),
        sa.Column('datasource_id', sa.Integer(), nullable=False),
        sa.Column('ts', sa.DateTime(), nullable=False),
        sa.Column('energy_kwh', sa.Float(), nullable=False),
        sa.Column('source_type', sa.String(length=32), nullable=False),
        sa.Column('source_batch_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('source_api_endpoint', sa.Text(), nullable=True),
        sa.Column('source_device_id', sa.String(length=64), nullable=True),
        sa.Column('ingestion_method', sa.String(length=32), nullable=False),
        sa.Column('ingested_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['datasource_id'], ['datasource.id'], ),
        sa.ForeignKeyConstraint(['source_batch_id'], ['ingest_batch.batch_id'], ),
        sa.PrimaryKeyConstraint('energy_id'),
        sa.UniqueConstraint('datasource_id', 'ts', name='uq_energy_hourly_datasource_ts')
    )
    op.create_index('idx_energy_hourly_batch', 'fact_energy_hourly', ['source_batch_id'], unique=False)
    op.create_index('idx_energy_hourly_datasource_ts', 'fact_energy_hourly', ['datasource_id', 'ts'], unique=False)
    op.create_index('idx_energy_hourly_source', 'fact_energy_hourly', ['source_type', 'source_device_id'], unique=False)

    # === Fact Energy Daily ===
    op.create_table('fact_energy_daily',
        sa.Column('energy_id', sa.Integer(), nullable=False),
        sa.Column('datasource_id', sa.Integer(), nullable=False),
        sa.Column('ts', sa.Date(), nullable=False),
        sa.Column('energy_kwh', sa.Float(), nullable=False),
        sa.Column('source_type', sa.String(length=32), nullable=False),
        sa.Column('source_batch_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('source_api_endpoint', sa.Text(), nullable=True),
        sa.Column('source_device_id', sa.String(length=64), nullable=True),
        sa.Column('ingestion_method', sa.String(length=32), nullable=False),
        sa.Column('ingested_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['datasource_id'], ['datasource.id'], ),
        sa.ForeignKeyConstraint(['source_batch_id'], ['ingest_batch.batch_id'], ),
        sa.PrimaryKeyConstraint('energy_id'),
        sa.UniqueConstraint('datasource_id', 'ts', name='uq_energy_daily_datasource_ts')
    )
    op.create_index('idx_energy_daily_batch', 'fact_energy_daily', ['source_batch_id'], unique=False)
    op.create_index('idx_energy_daily_datasource_ts', 'fact_energy_daily', ['datasource_id', 'ts'], unique=False)
    op.create_index('idx_energy_daily_source', 'fact_energy_daily', ['source_type', 'source_device_id'], unique=False)

    # === Environmental Metrics ===
    op.create_table('environmental_metrics',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('timestamp', sa.DateTime(timezone=True), nullable=False),
        sa.Column('atm_pressure', sa.Numeric(precision=8, scale=2), nullable=True),
        sa.Column('noise_level_db', sa.Numeric(precision=6, scale=2), nullable=True),
        sa.Column('temperature', sa.Numeric(precision=6, scale=2), nullable=True),
        sa.Column('humidity', sa.Numeric(precision=6, scale=2), nullable=True),
        sa.Column('pm10', sa.Numeric(precision=8, scale=2), nullable=True),
        sa.Column('wind_speed', sa.Numeric(precision=6, scale=2), nullable=True),
        sa.Column('wind_direction_sectors', sa.Numeric(precision=6, scale=2), nullable=True),
        sa.Column('wind_angle', sa.Numeric(precision=6, scale=2), nullable=True),
        sa.Column('pm2p5', sa.Numeric(precision=8, scale=2), nullable=True),
        sa.Column('source_type', sa.String(length=32), nullable=False),
        sa.Column('source_batch_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('source_api_endpoint', sa.Text(), nullable=True),
        sa.Column('source_device_id', sa.String(length=64), nullable=True),
        sa.Column('ingestion_method', sa.String(length=32), nullable=False),
        sa.Column('ingested_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['source_batch_id'], ['ingest_batch.batch_id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('timestamp')
    )
    op.create_index('idx_env_metrics_batch', 'environmental_metrics', ['source_batch_id'], unique=False)
    op.create_index('idx_env_metrics_source', 'environmental_metrics', ['source_type', 'source_device_id'], unique=False)
    op.create_index('idx_env_metrics_timestamp', 'environmental_metrics', ['timestamp'], unique=False)

    # === Dairy Production ===
    op.create_table('dairy_production',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('production_date', sa.Date(), nullable=False),
        sa.Column('day_production_per_cow_kg', sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column('number_of_animals', sa.Integer(), nullable=True),
        sa.Column('average_lactation_days', sa.Integer(), nullable=True),
        sa.Column('fed_per_cow_total_kg', sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column('fed_per_cow_water_kg', sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column('feed_efficiency', sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column('rumination_minutes', sa.Integer(), nullable=True),
        sa.Column('source_type', sa.String(length=32), nullable=False),
        sa.Column('source_batch_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('source_api_endpoint', sa.Text(), nullable=True),
        sa.Column('source_device_id', sa.String(length=64), nullable=True),
        sa.Column('ingestion_method', sa.String(length=32), nullable=False),
        sa.Column('ingested_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['source_batch_id'], ['ingest_batch.batch_id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('production_date')
    )
    op.create_index('idx_dairy_batch', 'dairy_production', ['source_batch_id'], unique=False)
    op.create_index('idx_dairy_production_date', 'dairy_production', ['production_date'], unique=False)
    op.create_index('idx_dairy_source', 'dairy_production', ['source_type', 'source_device_id'], unique=False)

    # === Staging Tables ===
    
    # Staging Environmental Metrics
    op.create_table('staging_environmental_metrics',
        sa.Column('staging_id', sa.Integer(), nullable=False),
        sa.Column('batch_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('row_number', sa.Integer(), nullable=False),
        sa.Column('raw_data', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('transformed_data', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('validation_errors', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('is_valid', sa.Boolean(), nullable=False),
        sa.Column('loaded_to_final', sa.Boolean(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['batch_id'], ['ingest_batch.batch_id'], ),
        sa.PrimaryKeyConstraint('staging_id')
    )
    op.create_index('idx_staging_env_batch', 'staging_environmental_metrics', ['batch_id'], unique=False)
    op.create_index('idx_staging_env_valid', 'staging_environmental_metrics', ['is_valid', 'loaded_to_final'], unique=False)

    # Staging Energy Hourly
    op.create_table('staging_energy_hourly',
        sa.Column('staging_id', sa.Integer(), nullable=False),
        sa.Column('batch_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('row_number', sa.Integer(), nullable=False),
        sa.Column('raw_data', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('transformed_data', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('validation_errors', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('is_valid', sa.Boolean(), nullable=False),
        sa.Column('loaded_to_final', sa.Boolean(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['batch_id'], ['ingest_batch.batch_id'], ),
        sa.PrimaryKeyConstraint('staging_id')
    )
    op.create_index('idx_staging_energy_hourly_batch', 'staging_energy_hourly', ['batch_id'], unique=False)
    op.create_index('idx_staging_energy_hourly_valid', 'staging_energy_hourly', ['is_valid', 'loaded_to_final'], unique=False)

    # Staging Energy Daily
    op.create_table('staging_energy_daily',
        sa.Column('staging_id', sa.Integer(), nullable=False),
        sa.Column('batch_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('row_number', sa.Integer(), nullable=False),
        sa.Column('raw_data', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('transformed_data', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('validation_errors', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('is_valid', sa.Boolean(), nullable=False),
        sa.Column('loaded_to_final', sa.Boolean(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['batch_id'], ['ingest_batch.batch_id'], ),
        sa.PrimaryKeyConstraint('staging_id')
    )
    op.create_index('idx_staging_energy_daily_batch', 'staging_energy_daily', ['batch_id'], unique=False)
    op.create_index('idx_staging_energy_daily_valid', 'staging_energy_daily', ['is_valid', 'loaded_to_final'], unique=False)

    # Staging Dairy Production
    op.create_table('staging_dairy_production',
        sa.Column('staging_id', sa.Integer(), nullable=False),
        sa.Column('batch_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('row_number', sa.Integer(), nullable=False),
        sa.Column('raw_data', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('transformed_data', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('validation_errors', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('is_valid', sa.Boolean(), nullable=False),
        sa.Column('loaded_to_final', sa.Boolean(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['batch_id'], ['ingest_batch.batch_id'], ),
        sa.PrimaryKeyConstraint('staging_id')
    )
    op.create_index('idx_staging_dairy_batch', 'staging_dairy_production', ['batch_id'], unique=False)
    op.create_index('idx_staging_dairy_valid', 'staging_dairy_production', ['is_valid', 'loaded_to_final'], unique=False)


def downgrade() -> None:
    # Drop staging tables
    op.drop_table('staging_dairy_production')
    op.drop_table('staging_energy_daily')
    op.drop_table('staging_energy_hourly')
    op.drop_table('staging_environmental_metrics')
    
    # Drop fact tables
    op.drop_table('dairy_production')
    op.drop_table('environmental_metrics')
    op.drop_table('fact_energy_daily')
    op.drop_table('fact_energy_hourly')
    
    # Drop pipeline tables
    op.drop_table('data_quality_check')
    op.drop_table('pipeline_execution')
    op.drop_table('api_fetch_cursor')
    
    # Drop core tables
    op.drop_table('ingest_batch')
    op.drop_table('datasource')
