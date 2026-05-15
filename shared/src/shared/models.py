"""
SQLAlchemy ORM Models for CEI-InOE

Single source of truth for database schema.
- API uses these directly via ORM
- Ingestor accesses Model.__table__ for Core operations
"""

from datetime import datetime, date
from typing import Optional
from uuid import uuid4

import enum

from sqlalchemy import (
    Column,
    Enum,
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
# Site Type Enum
# =============================================================================

class SiteType(str, enum.Enum):
    """Predefined types of sites."""
    DAIRY_FARM = "dairy_farm"
    FACTORY = "factory"
    OTHER = "other"


# =============================================================================
# Core Tables
# =============================================================================

class Site(Base):
    """
    Represents a physical site where the application is deployed.

    Populated from the configuration file on application startup.
    """
    __tablename__ = "site"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    location: Mapped[dict] = mapped_column(JSONB, nullable=False,
                                            comment="GeoJSON coordinates")
    site_type: Mapped[SiteType] = mapped_column(
        Enum(SiteType, name="site_type_enum", native_enum=False),
        nullable=False,
    )
    owner: Mapped[dict] = mapped_column(JSONB, nullable=False,
                                         comment="Owner details (person or organisation)")
    administrator_email: Mapped[str] = mapped_column(String(255), nullable=False,
                                                      comment="Email of the site administrator")

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(),
                                                  onupdate=func.now())

    # Relationships
    datasources: Mapped[list["Datasource"]] = relationship(back_populates="site")

    __table_args__ = (
        Index("idx_site_name", "name"),
    )


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
    
    # Site reference
    site_id: Mapped[Optional[int]] = mapped_column(ForeignKey("site.id"),
                                                    comment="Site this datasource belongs to")

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(),
                                                  onupdate=func.now())

    # Relationships
    site: Mapped[Optional["Site"]] = relationship(back_populates="datasources")
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
    ts: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    energy_kwh: Mapped[float] = mapped_column(Float, nullable=False)

    # Source tracking
    source_type: Mapped[str] = mapped_column(String(32), default="csv")
    source_batch_id: Mapped[Optional[UUID]] = mapped_column(UUID(as_uuid=True), 
                                                             ForeignKey("ingest_batch.batch_id"))
    source_api_endpoint: Mapped[Optional[str]] = mapped_column(Text)
    source_device_id: Mapped[Optional[str]] = mapped_column(String(64))
    datasource_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("datasource.id"))

    # Timestamps
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("source_batch_id", "ts", name="uq_energy_hourly_batch_ts"),
        UniqueConstraint("datasource_id", "ts", name="uq_energy_hourly_datasource_ts"),
        Index("idx_energy_hourly_ts", "ts"),
        Index("idx_energy_hourly_source", "source_type", "source_device_id"),
        Index("idx_energy_hourly_batch", "source_batch_id"),
        Index("idx_energy_hourly_datasource", "datasource_id"),
    )


class FactEnergyDaily(Base):
    """Daily energy consumption aggregates."""
    __tablename__ = "fact_energy_daily"

    energy_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ts: Mapped[date] = mapped_column(Date, nullable=False)
    energy_kwh: Mapped[float] = mapped_column(Float, nullable=False)

    # Source tracking
    source_type: Mapped[str] = mapped_column(String(32), default="csv")
    source_batch_id: Mapped[Optional[UUID]] = mapped_column(UUID(as_uuid=True),
                                                             ForeignKey("ingest_batch.batch_id"))
    source_api_endpoint: Mapped[Optional[str]] = mapped_column(Text)
    source_device_id: Mapped[Optional[str]] = mapped_column(String(64))
    datasource_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("datasource.id"))

    # Timestamps
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("source_batch_id", "ts", name="uq_energy_daily_batch_ts"),
        UniqueConstraint("datasource_id", "ts", name="uq_energy_daily_datasource_ts"),
        Index("idx_energy_daily_ts", "ts"),
        Index("idx_energy_daily_source", "source_type", "source_device_id"),
        Index("idx_energy_daily_batch", "source_batch_id"),
        Index("idx_energy_daily_datasource", "datasource_id"),
    )


class EnvironmentalMetrics(Base):
    """Environmental/weather sensor readings."""
    __tablename__ = "environmental_metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    
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
    datasource_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("datasource.id"))

    # Timestamps
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("source_batch_id", "timestamp", name="uq_env_metrics_batch_ts"),
        UniqueConstraint("datasource_id", "timestamp", name="uq_env_metrics_datasource_ts"),
        Index("idx_env_metrics_timestamp", "timestamp"),
        Index("idx_env_metrics_source", "source_type", "source_device_id"),
        Index("idx_env_metrics_batch", "source_batch_id"),
        Index("idx_env_metrics_datasource", "datasource_id"),
    )


class DairyProduction(Base):
    """Daily dairy production metrics."""
    __tablename__ = "dairy_production"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    production_date: Mapped[date] = mapped_column(Date, nullable=False)
    
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
    datasource_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("datasource.id"))

    # Timestamps
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("source_batch_id", "production_date", name="uq_dairy_batch_date"),
        UniqueConstraint("datasource_id", "production_date", name="uq_dairy_datasource_date"),
        Index("idx_dairy_production_date", "production_date"),
        Index("idx_dairy_source", "source_type", "source_device_id"),
        Index("idx_dairy_batch", "source_batch_id"),
        Index("idx_dairy_datasource", "datasource_id"),
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
# Weather Forecast Tables
# =============================================================================

class FactWeatherForecast(Base):
    """Hourly weather forecast data from Open-Meteo (or compatible providers).

    Each row is one forecast hour from one model run. The composite unique key
    (forecast_run_at, valid_at, site_id) lets multiple overlapping runs coexist
    so forecast accuracy can be evaluated across time horizons.
    """
    __tablename__ = "fact_weather_forecast"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # Temporal keys
    forecast_run_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        comment="When the NWP model run was issued")
    valid_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        comment="The hour being forecasted")
    horizon_hours: Mapped[int] = mapped_column(
        Integer, nullable=False,
        comment="valid_at − forecast_run_at in whole hours")

    # Location
    site_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("site.id"), nullable=False)

    # Solar radiation
    shortwave_radiation_wm2: Mapped[Optional[float]] = mapped_column(
        Float, comment="Global Horizontal Irradiance (W/m²)")
    direct_radiation_wm2: Mapped[Optional[float]] = mapped_column(
        Float, comment="Direct horizontal irradiance (W/m²)")
    direct_normal_irradiance_wm2: Mapped[Optional[float]] = mapped_column(
        Float, comment="DNI perpendicular to sun (W/m²)")
    diffuse_radiation_wm2: Mapped[Optional[float]] = mapped_column(
        Float, comment="Diffuse horizontal irradiance (W/m²)")
    global_tilted_irradiance_wm2: Mapped[Optional[float]] = mapped_column(
        Float, comment="GTI on panel tilt/azimuth (W/m²)")

    # Cloud cover
    cloud_cover_pct: Mapped[Optional[float]] = mapped_column(
        Float, comment="Total cloud cover (%)")
    cloud_cover_low_pct: Mapped[Optional[float]] = mapped_column(
        Float, comment="Low clouds 0–3 km (%)")
    cloud_cover_mid_pct: Mapped[Optional[float]] = mapped_column(
        Float, comment="Mid clouds 3–8 km (%)")
    cloud_cover_high_pct: Mapped[Optional[float]] = mapped_column(
        Float, comment="High clouds >8 km (%)")

    # Supporting weather
    temperature_2m_c: Mapped[Optional[float]] = mapped_column(
        Float, comment="Air temperature 2 m (°C)")
    wind_speed_10m_ms: Mapped[Optional[float]] = mapped_column(
        Float, comment="Wind speed 10 m (m/s)")
    wind_direction_10m_deg: Mapped[Optional[float]] = mapped_column(
        Float, comment="Wind direction 10 m (°)")
    precipitation_mm: Mapped[Optional[float]] = mapped_column(
        Float, comment="Precipitation preceding hour (mm)")
    weather_code: Mapped[Optional[int]] = mapped_column(
        Integer, comment="WMO weather interpretation code")

    # Derived
    sunshine_duration_s: Mapped[Optional[float]] = mapped_column(
        Float, comment="Seconds of sunshine in the hour")
    is_day: Mapped[Optional[bool]] = mapped_column(
        Boolean, comment="True if daylight, False if night")

    # Model metadata
    model_id: Mapped[str] = mapped_column(
        String(32), default="best_match",
        comment="NWP model used: best_match, icon, ifs, etc.")

    # Source tracking (standard pattern)
    source_type: Mapped[str] = mapped_column(String(32), default="api")
    source_batch_id: Mapped[Optional[UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("ingest_batch.batch_id"))
    source_api_endpoint: Mapped[Optional[str]] = mapped_column(Text)
    source_device_id: Mapped[Optional[str]] = mapped_column(String(64))

    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now())
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("forecast_run_at", "valid_at", "site_id",
                         name="uq_forecast_run_valid_site"),
        Index("idx_forecast_valid_at", "valid_at"),
        Index("idx_forecast_run", "forecast_run_at"),
        Index("idx_forecast_site", "site_id"),
        Index("idx_forecast_horizon", "horizon_hours"),
        Index("idx_forecast_batch", "source_batch_id"),
    )


class StagingWeatherForecast(Base):
    """Staging table for weather forecast ingestion."""
    __tablename__ = "staging_weather_forecast"

    staging_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    batch_id: Mapped[Optional[UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("ingest_batch.batch_id"))
    row_number: Mapped[int] = mapped_column(Integer, nullable=False)

    raw_data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    transformed_data: Mapped[Optional[dict]] = mapped_column(JSONB)
    validation_errors: Mapped[Optional[dict]] = mapped_column(JSONB)

    is_valid: Mapped[bool] = mapped_column(Boolean, default=False)
    loaded_to_final: Mapped[bool] = mapped_column(Boolean, default=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("idx_staging_forecast_batch", "batch_id"),
        Index("idx_staging_forecast_valid", "is_valid", "loaded_to_final"),
    )


class FactSolarHourly(Base):
    """Hourly KPIs from FusionSolar API."""
    __tablename__ = "fact_solar_hourly"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # Generation
    pv_yield_kwh: Mapped[Optional[float]] = mapped_column(Float, comment="PV generation (kWh)")
    inverter_yield_kwh: Mapped[Optional[float]] = mapped_column(Float, comment="Inverter yield (kWh)")
    inverter_power_kw: Mapped[Optional[float]] = mapped_column(Float, comment="Inverter power (kW)")

    # Grid exchange
    ongrid_power_kwh: Mapped[Optional[float]] = mapped_column(Float, comment="Exported to grid (kWh)")
    buy_power_kwh: Mapped[Optional[float]] = mapped_column(Float, comment="Imported from grid (kWh)")

    # Consumption
    use_power_kwh: Mapped[Optional[float]] = mapped_column(Float, comment="Site consumption (kWh)")
    self_use_power_kwh: Mapped[Optional[float]] = mapped_column(Float, comment="PV self-consumed (kWh)")

    # Ratios
    self_provide_pct: Mapped[Optional[float]] = mapped_column(Float, comment="Self-sufficiency (%)")
    perpower_ratio: Mapped[Optional[float]] = mapped_column(Float, comment="Specific yield (kWh/kWp)")

    # Plant metadata
    installed_capacity_kwp: Mapped[Optional[float]] = mapped_column(Float, comment="Installed capacity (kWp)")

    # Financial / environmental
    power_profit: Mapped[Optional[float]] = mapped_column(Float, comment="Revenue / savings")
    reduction_total_co2: Mapped[Optional[float]] = mapped_column(Float, comment="CO2 avoided")
    reduction_total_coal: Mapped[Optional[float]] = mapped_column(Float, comment="Coal avoided")
    reduction_total_tree: Mapped[Optional[float]] = mapped_column(Float, comment="Equivalent trees")

    # Source tracking
    source_type: Mapped[str] = mapped_column(String(32), default="api")
    source_batch_id: Mapped[Optional[UUID]] = mapped_column(UUID(as_uuid=True),
                                                             ForeignKey("ingest_batch.batch_id"))
    source_api_endpoint: Mapped[Optional[str]] = mapped_column(Text)
    source_device_id: Mapped[Optional[str]] = mapped_column(String(64))
    datasource_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("datasource.id"))

    # Timestamps
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("source_batch_id", "ts", name="uq_solar_hourly_batch_ts"),
        UniqueConstraint("datasource_id", "ts", name="uq_solar_hourly_datasource_ts"),
        Index("idx_solar_hourly_ts", "ts"),
        Index("idx_solar_hourly_source", "source_type", "source_device_id"),
        Index("idx_solar_hourly_batch", "source_batch_id"),
        Index("idx_solar_hourly_datasource", "datasource_id"),
    )


class FactSolarDaily(Base):
    """Daily KPIs from FusionSolar API."""
    __tablename__ = "fact_solar_daily"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ts: Mapped[date] = mapped_column(Date, nullable=False)

    # Generation
    pv_yield_kwh: Mapped[Optional[float]] = mapped_column(Float, comment="PV generation (kWh)")
    inverter_yield_kwh: Mapped[Optional[float]] = mapped_column(Float, comment="Inverter yield (kWh)")
    inverter_power_kw: Mapped[Optional[float]] = mapped_column(Float, comment="Inverter power (kW)")

    # Grid exchange
    ongrid_power_kwh: Mapped[Optional[float]] = mapped_column(Float, comment="Exported to grid (kWh)")
    buy_power_kwh: Mapped[Optional[float]] = mapped_column(Float, comment="Imported from grid (kWh)")

    # Consumption
    use_power_kwh: Mapped[Optional[float]] = mapped_column(Float, comment="Site consumption (kWh)")
    self_use_power_kwh: Mapped[Optional[float]] = mapped_column(Float, comment="PV self-consumed (kWh)")

    # Ratios
    self_provide_pct: Mapped[Optional[float]] = mapped_column(Float, comment="Self-sufficiency (%)")
    perpower_ratio: Mapped[Optional[float]] = mapped_column(Float, comment="Specific yield (kWh/kWp)")

    # Plant metadata
    installed_capacity_kwp: Mapped[Optional[float]] = mapped_column(Float, comment="Installed capacity (kWp)")

    # Financial / environmental
    power_profit: Mapped[Optional[float]] = mapped_column(Float, comment="Revenue / savings")
    reduction_total_co2: Mapped[Optional[float]] = mapped_column(Float, comment="CO2 avoided")
    reduction_total_coal: Mapped[Optional[float]] = mapped_column(Float, comment="Coal avoided")
    reduction_total_tree: Mapped[Optional[float]] = mapped_column(Float, comment="Equivalent trees")

    # Source tracking
    source_type: Mapped[str] = mapped_column(String(32), default="api")
    source_batch_id: Mapped[Optional[UUID]] = mapped_column(UUID(as_uuid=True),
                                                             ForeignKey("ingest_batch.batch_id"))
    source_api_endpoint: Mapped[Optional[str]] = mapped_column(Text)
    source_device_id: Mapped[Optional[str]] = mapped_column(String(64))
    datasource_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("datasource.id"))

    # Timestamps
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("source_batch_id", "ts", name="uq_solar_daily_batch_ts"),
        UniqueConstraint("datasource_id", "ts", name="uq_solar_daily_datasource_ts"),
        Index("idx_solar_daily_ts", "ts"),
        Index("idx_solar_daily_source", "source_type", "source_device_id"),
        Index("idx_solar_daily_batch", "source_batch_id"),
        Index("idx_solar_daily_datasource", "datasource_id"),
    )


class FactSolarMonthly(Base):
    """Monthly KPIs from FusionSolar API."""
    __tablename__ = "fact_solar_monthly"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ts: Mapped[date] = mapped_column(Date, nullable=False)

    # Generation
    pv_yield_kwh: Mapped[Optional[float]] = mapped_column(Float, comment="PV generation (kWh)")
    inverter_yield_kwh: Mapped[Optional[float]] = mapped_column(Float, comment="Inverter yield (kWh)")
    inverter_power_kw: Mapped[Optional[float]] = mapped_column(Float, comment="Inverter power (kW)")

    # Grid exchange
    ongrid_power_kwh: Mapped[Optional[float]] = mapped_column(Float, comment="Exported to grid (kWh)")
    buy_power_kwh: Mapped[Optional[float]] = mapped_column(Float, comment="Imported from grid (kWh)")

    # Consumption
    use_power_kwh: Mapped[Optional[float]] = mapped_column(Float, comment="Site consumption (kWh)")
    self_use_power_kwh: Mapped[Optional[float]] = mapped_column(Float, comment="PV self-consumed (kWh)")

    # Ratios
    self_provide_pct: Mapped[Optional[float]] = mapped_column(Float, comment="Self-sufficiency (%)")
    perpower_ratio: Mapped[Optional[float]] = mapped_column(Float, comment="Specific yield (kWh/kWp)")

    # Plant metadata
    installed_capacity_kwp: Mapped[Optional[float]] = mapped_column(Float, comment="Installed capacity (kWp)")

    # Financial / environmental
    power_profit: Mapped[Optional[float]] = mapped_column(Float, comment="Revenue / savings")
    reduction_total_co2: Mapped[Optional[float]] = mapped_column(Float, comment="CO2 avoided")
    reduction_total_coal: Mapped[Optional[float]] = mapped_column(Float, comment="Coal avoided")
    reduction_total_tree: Mapped[Optional[float]] = mapped_column(Float, comment="Equivalent trees")

    # Source tracking
    source_type: Mapped[str] = mapped_column(String(32), default="api")
    source_batch_id: Mapped[Optional[UUID]] = mapped_column(UUID(as_uuid=True),
                                                             ForeignKey("ingest_batch.batch_id"))
    source_api_endpoint: Mapped[Optional[str]] = mapped_column(Text)
    source_device_id: Mapped[Optional[str]] = mapped_column(String(64))
    datasource_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("datasource.id"))

    # Timestamps
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("source_batch_id", "ts", name="uq_solar_monthly_batch_ts"),
        UniqueConstraint("datasource_id", "ts", name="uq_solar_monthly_datasource_ts"),
        Index("idx_solar_monthly_ts", "ts"),
        Index("idx_solar_monthly_source", "source_type", "source_device_id"),
        Index("idx_solar_monthly_batch", "source_batch_id"),
        Index("idx_solar_monthly_datasource", "datasource_id"),
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


class StagingSolarKpi(Base):
    """Unified staging table for all solar KPI ingestion (hourly/daily/monthly)."""
    __tablename__ = "staging_solar_kpi"

    staging_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    batch_id: Mapped[Optional[UUID]] = mapped_column(UUID(as_uuid=True),
                                                      ForeignKey("ingest_batch.batch_id"))
    row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    granularity: Mapped[str] = mapped_column(String(16), nullable=False, comment="hourly | daily | monthly")

    raw_data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    transformed_data: Mapped[Optional[dict]] = mapped_column(JSONB)
    validation_errors: Mapped[Optional[dict]] = mapped_column(JSONB)

    is_valid: Mapped[bool] = mapped_column(Boolean, default=False)
    loaded_to_final: Mapped[bool] = mapped_column(Boolean, default=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("idx_staging_solar_kpi_batch", "batch_id"),
        Index("idx_staging_solar_kpi_valid", "is_valid", "loaded_to_final"),
        Index("idx_staging_solar_kpi_granularity", "granularity"),
    )
