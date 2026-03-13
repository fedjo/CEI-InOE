"""
SQLAlchemy ORM Models for CEI-InOE

Single source of truth for database schema.
- API uses these directly via ORM
- Ingestor accesses Model.__table__ for Core operations
"""

from datetime import datetime, date
from typing import Optional
from uuid import uuid4

from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    Boolean,
    DateTime,
    Date,
    Text,
    Numeric,
    ForeignKey,
    Index,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all models."""
    pass


# =============================================================================
# Core Tables
# =============================================================================

class Datasource(Base):
    """
    Unified data source registry.
    
    Replaces the old 'device' and 'generic_device' tables.
    Supports devices, files, APIs, and other data sources.
    """
    __tablename__ = "datasource"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    external_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False,
                                              comment="External identifier (API ID, serial number, etc.)")
    
    # Classification
    source_category: Mapped[str] = mapped_column(String(32), nullable=False,
                                                  comment="Category: device, file, api, manual")
    data_type: Mapped[str] = mapped_column(String(32), nullable=False,
                                           comment="Type: energy, weather, dairy, pv")
    
    # Display
    name: Mapped[Optional[str]] = mapped_column(String(255))
    alias: Mapped[Optional[str]] = mapped_column(String(255))
    client: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    
    # State
    status: Mapped[str] = mapped_column(String(32), default="active",
                                        comment="Status: active, inactive, deprecated")
    timezone: Mapped[str] = mapped_column(String(64), default="UTC")
    
    # Configuration (type-specific metadata)
    metadata_: Mapped[dict] = mapped_column("metadata", JSONB, default=dict,
                                            comment="Type-specific configuration and metadata")
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(),
                                                  onupdate=func.now())

    # Relationships
    ingest_batches: Mapped[list["IngestBatch"]] = relationship(back_populates="datasource")
    
    __table_args__ = (
        Index("idx_datasource_external_id", "external_id"),
        Index("idx_datasource_metadata", "metadata", postgresql_using="gin"),
    )


class IngestBatch(Base):
    """
    Ingestion batch tracking.
    
    Replaces the old 'ingest_file' table.
    Represents a unit of work for data ingestion (file upload, API fetch, etc.)
    """
    __tablename__ = "ingest_batch"

    batch_id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    
    # Source identification
    source_type: Mapped[str] = mapped_column(String(32), nullable=False,
                                              comment="Source type: file, api, stream, manual")
    source_name: Mapped[str] = mapped_column(Text, nullable=False,
                                              comment="Filename, API endpoint, or description")
    
    # Linked datasource (optional)
    datasource_id: Mapped[Optional[int]] = mapped_column(ForeignKey("datasource.id"))

    # File-specific (optional)
    file_sha256: Mapped[Optional[str]] = mapped_column(String(64), unique=True,
                                                        comment="File hash for deduplication")
    
    # Data range
    granularity: Mapped[Optional[str]] = mapped_column(String(32),
                                                        comment="Data granularity: hourly, daily")
    date_range_start: Mapped[Optional[date]] = mapped_column(Date)
    date_range_end: Mapped[Optional[date]] = mapped_column(Date)
    
    # Processing state
    status: Mapped[str] = mapped_column(String(32), default="pending",
                                        comment="Status: pending, processing, completed, failed")
    records_loaded: Mapped[int] = mapped_column(Integer, default=0)
    records_failed: Mapped[int] = mapped_column(Integer, default=0)
    
    # Metrics
    execution_time_ms: Mapped[Optional[int]] = mapped_column(Integer)
    validation_status: Mapped[Optional[str]] = mapped_column(String(32),
                                                              comment="Validation: passed, failed, partial")
    quality_score: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))
    pipeline_version: Mapped[str] = mapped_column(String(32), default="1.0")
    
    # Timestamps
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Relationships
    datasource: Mapped[Optional["Datasource"]] = relationship(back_populates="ingest_batches")
    pipeline_executions: Mapped[list["PipelineExecution"]] = relationship(back_populates="ingest_batch")
    quality_checks: Mapped[list["DataQualityCheck"]] = relationship(back_populates="ingest_batch")

    __table_args__ = (
        Index("idx_ingest_batch_source_type", "source_type"),
        Index("idx_ingest_batch_status", "status"),
        Index("idx_ingest_batch_datasource", "datasource_id"),
        Index("idx_ingest_batch_validation", "validation_status", "started_at"),
    )


# =============================================================================
# Fact Tables
# =============================================================================

class FactEnergyHourly(Base):
    """Hourly energy consumption readings."""
    __tablename__ = "fact_energy_hourly"

    energy_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ts: Mapped[datetime] = mapped_column(DateTime, nullable=False, unique=True)
    energy_kwh: Mapped[float] = mapped_column(Float, nullable=False)

    # Source tracking
    source_type: Mapped[str] = mapped_column(String(32), default="csv")
    source_batch_id: Mapped[Optional[UUID]] = mapped_column(UUID(as_uuid=True), 
                                                             ForeignKey("ingest_batch.batch_id"))
    source_api_endpoint: Mapped[Optional[str]] = mapped_column(Text)
    source_device_id: Mapped[Optional[str]] = mapped_column(String(64))

    # Timestamps
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("idx_energy_hourly_source", "source_type", "source_device_id"),
        Index("idx_energy_hourly_batch", "source_batch_id"),
    )


class FactEnergyDaily(Base):
    """Daily energy consumption aggregates."""
    __tablename__ = "fact_energy_daily"

    energy_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ts: Mapped[date] = mapped_column(Date, nullable=False, unique=True)
    energy_kwh: Mapped[float] = mapped_column(Float, nullable=False)

    # Source tracking
    source_type: Mapped[str] = mapped_column(String(32), default="csv")
    source_batch_id: Mapped[Optional[UUID]] = mapped_column(UUID(as_uuid=True),
                                                             ForeignKey("ingest_batch.batch_id"))
    source_api_endpoint: Mapped[Optional[str]] = mapped_column(Text)
    source_device_id: Mapped[Optional[str]] = mapped_column(String(64))

    # Timestamps
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("idx_energy_daily_source", "source_type", "source_device_id"),
        Index("idx_energy_daily_batch", "source_batch_id"),
    )


class EnvironmentalMetrics(Base):
    """Environmental/weather sensor readings."""
    __tablename__ = "environmental_metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, unique=True)
    
    # Measurements
    atm_pressure: Mapped[Optional[float]] = mapped_column(Numeric(8, 2))
    noise_level_db: Mapped[Optional[float]] = mapped_column(Numeric(6, 2))
    temperature: Mapped[Optional[float]] = mapped_column(Numeric(6, 2))
    humidity: Mapped[Optional[float]] = mapped_column(Numeric(6, 2))
    pm10: Mapped[Optional[float]] = mapped_column(Numeric(8, 2))
    wind_speed: Mapped[Optional[float]] = mapped_column(Numeric(6, 2))
    wind_direction_sectors: Mapped[Optional[float]] = mapped_column(Numeric(6, 2))
    wind_angle: Mapped[Optional[float]] = mapped_column(Numeric(6, 2))
    pm2p5: Mapped[Optional[float]] = mapped_column(Numeric(8, 2))
    
    # Source tracking
    source_type: Mapped[str] = mapped_column(String(32), default="csv")
    source_batch_id: Mapped[Optional[UUID]] = mapped_column(UUID(as_uuid=True),
                                                             ForeignKey("ingest_batch.batch_id"))
    source_api_endpoint: Mapped[Optional[str]] = mapped_column(Text)
    source_device_id: Mapped[Optional[str]] = mapped_column(String(64))

    # Timestamps
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("idx_env_metrics_timestamp", "timestamp"),
        Index("idx_env_metrics_source", "source_type", "source_device_id"),
        Index("idx_env_metrics_batch", "source_batch_id"),
    )


class DairyProduction(Base):
    """Daily dairy production metrics."""
    __tablename__ = "dairy_production"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    production_date: Mapped[date] = mapped_column(Date, nullable=False, unique=True)
    
    # Production metrics
    day_production_per_cow_kg: Mapped[Optional[float]] = mapped_column(Numeric(10, 2))
    number_of_animals: Mapped[Optional[int]] = mapped_column(Integer)
    average_lactation_days: Mapped[Optional[int]] = mapped_column(Integer)
    
    # Feed metrics
    fed_per_cow_total_kg: Mapped[Optional[float]] = mapped_column(Numeric(10, 2))
    fed_per_cow_water_kg: Mapped[Optional[float]] = mapped_column(Numeric(10, 2))
    feed_efficiency: Mapped[Optional[float]] = mapped_column(Numeric(10, 4))
    rumination_minutes: Mapped[Optional[int]] = mapped_column(Integer)
    
    # Source tracking
    source_type: Mapped[str] = mapped_column(String(32), default="csv")
    source_batch_id: Mapped[Optional[UUID]] = mapped_column(UUID(as_uuid=True),
                                                             ForeignKey("ingest_batch.batch_id"))
    source_api_endpoint: Mapped[Optional[str]] = mapped_column(Text)
    source_device_id: Mapped[Optional[str]] = mapped_column(String(64))

    # Timestamps
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("idx_dairy_production_date", "production_date"),
        Index("idx_dairy_source", "source_type", "source_device_id"),
        Index("idx_dairy_batch", "source_batch_id"),
    )


# =============================================================================
# Pipeline & Quality Tables
# =============================================================================

class PipelineExecution(Base):
    """Pipeline stage execution tracking."""
    __tablename__ = "pipeline_execution"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    batch_id: Mapped[Optional[UUID]] = mapped_column(UUID(as_uuid=True),
                                                      ForeignKey("ingest_batch.batch_id"))
    pipeline_name: Mapped[str] = mapped_column(Text, nullable=False)
    stage: Mapped[str] = mapped_column(Text, nullable=False,
                                       comment="Stage: extract, validate, transform, load")
    
    # Timing
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    
    # Status & metrics
    status: Mapped[str] = mapped_column(Text, nullable=False,
                                        comment="Status: running, success, failed, skipped")
    records_in: Mapped[int] = mapped_column(Integer, default=0)
    records_out: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    execution_metadata: Mapped[Optional[dict]] = mapped_column(JSONB)
    
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    ingest_batch: Mapped[Optional["IngestBatch"]] = relationship(back_populates="pipeline_executions")

    __table_args__ = (
        Index("idx_pipeline_exec_batch", "batch_id"),
        Index("idx_pipeline_exec_stage", "stage", "status"),
        Index("idx_pipeline_exec_started", "started_at"),
    )


class DataQualityCheck(Base):
    """Data quality validation results."""
    __tablename__ = "data_quality_check"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    batch_id: Mapped[Optional[UUID]] = mapped_column(UUID(as_uuid=True),
                                                      ForeignKey("ingest_batch.batch_id"))
    dataset: Mapped[str] = mapped_column(Text, nullable=False)
    
    # Check details
    check_type: Mapped[str] = mapped_column(Text, nullable=False,
                                            comment="Type: schema, type, range, uniqueness, completeness")
    check_name: Mapped[str] = mapped_column(Text, nullable=False)
    passed: Mapped[bool] = mapped_column(Boolean, nullable=False)
    
    # Metrics
    failed_count: Mapped[int] = mapped_column(Integer, default=0)
    total_count: Mapped[int] = mapped_column(Integer, default=0)
    failure_rate: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))
    sample_failures: Mapped[Optional[dict]] = mapped_column(JSONB)
    
    checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    ingest_batch: Mapped[Optional["IngestBatch"]] = relationship(back_populates="quality_checks")

    __table_args__ = (
        Index("idx_quality_check_batch", "batch_id"),
        Index("idx_quality_check_dataset", "dataset", "check_type"),
        Index("idx_quality_check_passed", "passed", "checked_at"),
    )


class ApiFetchCursor(Base):
    """API incremental fetch cursor tracking."""
    __tablename__ = "api_fetch_cursor"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connector_id: Mapped[str] = mapped_column(Text, nullable=False)
    endpoint_id: Mapped[str] = mapped_column(Text, nullable=False)
    datasource_id: Mapped[Optional[int]] = mapped_column(ForeignKey("datasource.id"))

    # Cursor state
    last_fetch_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_fetch_success: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    fetch_count: Mapped[int] = mapped_column(Integer, default=1)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(),
                                                  onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("connector_id", "endpoint_id", "datasource_id",
                        name="uq_api_cursor_connector_endpoint_datasource"),
        Index("idx_api_cursor_lookup", "connector_id", "endpoint_id", "datasource_id"),
    )


# =============================================================================
# Staging Tables
# =============================================================================

class StagingEnvironmentalMetrics(Base):
    """Staging table for environmental metrics ingestion."""
    __tablename__ = "staging_environmental_metrics"

    staging_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    batch_id: Mapped[Optional[UUID]] = mapped_column(UUID(as_uuid=True),
                                                      ForeignKey("ingest_batch.batch_id"))
    row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    
    # Data
    raw_data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    transformed_data: Mapped[Optional[dict]] = mapped_column(JSONB)
    validation_errors: Mapped[Optional[dict]] = mapped_column(JSONB)
    
    # State
    is_valid: Mapped[bool] = mapped_column(Boolean, default=False)
    loaded_to_final: Mapped[bool] = mapped_column(Boolean, default=False)
    
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("idx_staging_env_batch", "batch_id"),
        Index("idx_staging_env_valid", "is_valid", "loaded_to_final"),
    )


class StagingEnergyHourly(Base):
    """Staging table for hourly energy data ingestion."""
    __tablename__ = "staging_energy_hourly"

    staging_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    batch_id: Mapped[Optional[UUID]] = mapped_column(UUID(as_uuid=True),
                                                      ForeignKey("ingest_batch.batch_id"))
    row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    
    # Data
    raw_data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    transformed_data: Mapped[Optional[dict]] = mapped_column(JSONB)
    validation_errors: Mapped[Optional[dict]] = mapped_column(JSONB)
    
    # State
    is_valid: Mapped[bool] = mapped_column(Boolean, default=False)
    loaded_to_final: Mapped[bool] = mapped_column(Boolean, default=False)
    
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("idx_staging_energy_hourly_batch", "batch_id"),
        Index("idx_staging_energy_hourly_valid", "is_valid", "loaded_to_final"),
    )


class StagingEnergyDaily(Base):
    """Staging table for daily energy data ingestion."""
    __tablename__ = "staging_energy_daily"

    staging_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    batch_id: Mapped[Optional[UUID]] = mapped_column(UUID(as_uuid=True),
                                                      ForeignKey("ingest_batch.batch_id"))
    row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    
    # Data
    raw_data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    transformed_data: Mapped[Optional[dict]] = mapped_column(JSONB)
    validation_errors: Mapped[Optional[dict]] = mapped_column(JSONB)
    
    # State
    is_valid: Mapped[bool] = mapped_column(Boolean, default=False)
    loaded_to_final: Mapped[bool] = mapped_column(Boolean, default=False)
    
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("idx_staging_energy_daily_batch", "batch_id"),
        Index("idx_staging_energy_daily_valid", "is_valid", "loaded_to_final"),
    )


class StagingDairyProduction(Base):
    """Staging table for dairy production data ingestion."""
    __tablename__ = "staging_dairy_production"

    staging_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    batch_id: Mapped[Optional[UUID]] = mapped_column(UUID(as_uuid=True),
                                                      ForeignKey("ingest_batch.batch_id"))
    row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    
    # Data
    raw_data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    transformed_data: Mapped[Optional[dict]] = mapped_column(JSONB)
    validation_errors: Mapped[Optional[dict]] = mapped_column(JSONB)
    
    # State
    is_valid: Mapped[bool] = mapped_column(Boolean, default=False)
    loaded_to_final: Mapped[bool] = mapped_column(Boolean, default=False)
    
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("idx_staging_dairy_batch", "batch_id"),
        Index("idx_staging_dairy_valid", "is_valid", "loaded_to_final"),
    )
