"""
Datasource queries using SQLAlchemy ORM.

Replaces the old devices.py queries.
"""

from sqlalchemy import func, delete, select
from sqlalchemy.orm import Session

from shared import (
    Datasource,
    IngestBatch,
    FactEnergyHourly,
    FactEnergyDaily,
    EnvironmentalMetrics,
    DairyProduction,
    FactSolarHourly,
    FactSolarDaily,
    FactSolarMonthly,
    ApiFetchCursor,
    PipelineExecution,
    DataQualityCheck,
    StagingEnvironmentalMetrics,
    StagingEnergyHourly,
    StagingEnergyDaily,
    StagingDairyProduction,
    StagingSolarKpi,
    StagingWeatherForecast,
)


def get_datasources(
    db: Session,
    data_type: str | None = None,
    source_category: str | None = None,
    status: str | None = None,
    site_id: int | None = None,
    datasource_ids: list[int] | None = None,
    page: int = 1,
    page_size: int = 100
) -> tuple[list[Datasource], int]:
    """
    Get datasources with optional filters.

    Args:
        datasource_ids: If provided, restrict results to these IDs (used to
            scope results to a principal's allowed datasources).
    
    Returns:
        Tuple of (datasource_list, total_count)
    """
    query = db.query(Datasource)
    
    if data_type:
        query = query.filter(Datasource.data_type == data_type)
    
    if source_category:
        query = query.filter(Datasource.source_category == source_category)
    
    if status:
        query = query.filter(Datasource.status == status)

    if site_id is not None:
        query = query.filter(Datasource.site_id == site_id)

    if datasource_ids is not None:
        query = query.filter(Datasource.id.in_(datasource_ids))
    
    # Get total count
    total = query.count()
    
    # Apply pagination
    datasources = query.order_by(Datasource.created_at.desc()) \
        .offset((page - 1) * page_size) \
        .limit(page_size) \
        .all()
    
    return datasources, total


def get_datasource_by_id(db: Session, datasource_id: int) -> Datasource | None:
    """Get datasource by internal ID."""
    return db.query(Datasource).filter(Datasource.id == datasource_id).first()


def get_datasource_by_external_id(db: Session, external_id: str) -> Datasource | None:
    """Get datasource by external ID."""
    return db.query(Datasource).filter(Datasource.external_id == external_id).first()


def get_data_types(db: Session) -> list[dict]:
    """Get distinct data types with counts."""
    results = db.query(
        Datasource.data_type,
        func.count(Datasource.id).label('count')
    ).group_by(Datasource.data_type) \
     .order_by(Datasource.data_type) \
     .all()
    
    return [{"data_type": r.data_type, "count": r.count} for r in results]


def get_source_categories(db: Session) -> list[dict]:
    """Get distinct source categories with counts."""
    results = db.query(
        Datasource.source_category,
        func.count(Datasource.id).label('count')
    ).group_by(Datasource.source_category) \
     .order_by(Datasource.source_category) \
     .all()
    
    return [{"source_category": r.source_category, "count": r.count} for r in results]


def create_datasource(db: Session, datasource: Datasource) -> Datasource:
    """Create a new datasource."""
    db.add(datasource)
    db.commit()
    db.refresh(datasource)
    return datasource


def update_datasource(db: Session, datasource_id: int, **kwargs) -> Datasource | None:
    """Update datasource fields."""
    datasource = get_datasource_by_id(db, datasource_id)
    if not datasource:
        return None
    
    for key, value in kwargs.items():
        if hasattr(datasource, key) and value is not None:
            setattr(datasource, key, value)
    
    db.commit()
    db.refresh(datasource)
    return datasource


def soft_delete_datasource(db: Session, datasource_id: int) -> Datasource | None:
    """Set datasource status to 'offline'. Returns updated row or None if not found."""
    return update_datasource(db, datasource_id, status="offline")


def purge_datasource(db: Session, datasource_id: int) -> dict[str, int]:
    """
    Hard-delete a datasource and ALL related data in one transaction.

    Deletion order respects FK constraints (children before parents).
    Returns a dict mapping table name → rows deleted.
    """
    stats: dict[str, int] = {}

    # 1. Collect batch IDs belonging to this datasource so staging/pipeline
    #    rows (which FK to ingest_batch, not to datasource) can be cleaned up.
    batch_ids = db.scalars(
        select(IngestBatch.batch_id).where(IngestBatch.datasource_id == datasource_id)
    ).all()

    # 2. Fact tables — direct FK to datasource
    for Model in (
        FactEnergyHourly,
        FactEnergyDaily,
        EnvironmentalMetrics,
        DairyProduction,
        FactSolarHourly,
        FactSolarDaily,
        FactSolarMonthly,
    ):
        result = db.execute(
            delete(Model).where(Model.datasource_id == datasource_id)
        )
        stats[Model.__tablename__] = result.rowcount

    # 3. Staging tables — FK to ingest_batch
    if batch_ids:
        for Model in (
            StagingEnvironmentalMetrics,
            StagingEnergyHourly,
            StagingEnergyDaily,
            StagingDairyProduction,
            StagingSolarKpi,
            StagingWeatherForecast,
        ):
            result = db.execute(
                delete(Model).where(Model.batch_id.in_(batch_ids))
            )
            stats[Model.__tablename__] = result.rowcount

        # 4. Pipeline and quality rows — FK to ingest_batch
        for Model in (PipelineExecution, DataQualityCheck):
            result = db.execute(
                delete(Model).where(Model.batch_id.in_(batch_ids))
            )
            stats[Model.__tablename__] = result.rowcount

    # 5. API fetch cursors — FK to datasource
    result = db.execute(
        delete(ApiFetchCursor).where(ApiFetchCursor.datasource_id == datasource_id)
    )
    stats[ApiFetchCursor.__tablename__] = result.rowcount

    # 6. Ingest batches — FK to datasource
    result = db.execute(
        delete(IngestBatch).where(IngestBatch.datasource_id == datasource_id)
    )
    stats[IngestBatch.__tablename__] = result.rowcount

    # 7. Datasource row — last
    result = db.execute(
        delete(Datasource).where(Datasource.id == datasource_id)
    )
    stats[Datasource.__tablename__] = result.rowcount

    db.commit()
    return stats
