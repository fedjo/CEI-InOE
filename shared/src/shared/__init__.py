"""
CEI-InOE Shared Package

Provides SQLAlchemy models and Pydantic schemas shared between api/ and ingestor/.
"""

from shared.models import (
    Base,
    SiteType,
    Site,
    Datasource,
    IngestBatch,
    FactEnergyHourly,
    FactEnergyDaily,
    EnvironmentalMetrics,
    DairyProduction,
    PipelineExecution,
    DataQualityCheck,
    ApiFetchCursor,
    StagingEnvironmentalMetrics,
    StagingEnergyHourly,
    StagingEnergyDaily,
    StagingDairyProduction,
)

from shared.schemas import (
    GeoJSONPoint,
    SiteRead,
    SiteCreate,
    SiteUpdate,
    DatasourceRead,
    DatasourceCreate,
    DatasourceUpdate,
    DatasourceTypeCount,
    IngestBatchRead,
    IngestBatchCreate,
    IngestBatchSummary,
    EnergyHourlyRead,
    EnergyDailyRead,
    EnvironmentalMetricsRead,
    EnvironmentalMetricsDailySummary,
    DairyProductionRead,
    DairyProductionMonthlySummary,
    PipelineExecutionRead,
    DataQualityCheckRead,
    PaginatedResponse,
    HealthResponse,
)

from shared.database import (
    get_engine,
    close_engine,
    get_session,
    session_scope,
    get_connection,
    check_connection,
    create_all_tables,
)


__all__ = [
    # Base
    "Base",
    # Models
    "SiteType",
    "Site",
    "Datasource",
    "IngestBatch",
    "FactEnergyHourly",
    "FactEnergyDaily",
    "EnvironmentalMetrics",
    "DairyProduction",
    "PipelineExecution",
    "DataQualityCheck",
    "ApiFetchCursor",
    "StagingEnvironmentalMetrics",
    "StagingEnergyHourly",
    "StagingEnergyDaily",
    "StagingDairyProduction",
    # Schemas
    "GeoJSONPoint",
    "SiteRead",
    "SiteCreate",
    "SiteUpdate",
    "DatasourceRead",
    "DatasourceCreate",
    "DatasourceUpdate",
    "DatasourceTypeCount",
    "IngestBatchRead",
    "IngestBatchCreate",
    "IngestBatchSummary",
    "EnergyHourlyRead",
    "EnergyDailyRead",
    "EnvironmentalMetricsRead",
    "EnvironmentalMetricsDailySummary",
    "DairyProductionRead",
    "DairyProductionMonthlySummary",
    "PipelineExecutionRead",
    "DataQualityCheckRead",
    "PaginatedResponse",
    "HealthResponse",
    # Database utilities
    "get_engine",
    "close_engine",
    "get_session",
    "session_scope",
    "get_connection",
    "check_connection",
    "create_all_tables",
]
