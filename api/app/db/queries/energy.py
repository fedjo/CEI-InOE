"""Energy queries."""

from datetime import date, datetime
from sqlalchemy import func
from sqlalchemy.orm import Session

from shared import FactEnergyHourly, FactEnergyDaily, IngestBatch


def get_hourly(
    db: Session,
    start_date: date,
    end_date: date,
    datasource_id: int | None = None,
    page: int = 1,
    page_size: int = 100
) -> tuple[list[FactEnergyHourly], int]:
    """
    Get hourly energy records with optional filters.
    
    Returns:
        Tuple of (records, total_count)
    """
    # Convert dates to datetime for comparison
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())
    
    query = db.query(FactEnergyHourly).filter(
        FactEnergyHourly.ts >= start_dt,
        FactEnergyHourly.ts <= end_dt
    )
    
    if datasource_id:
        query = query.join(
            IngestBatch, FactEnergyHourly.source_batch_id == IngestBatch.batch_id
        ).filter(IngestBatch.datasource_id == datasource_id)
    
    total = query.count()
    
    records = query.order_by(FactEnergyHourly.ts.desc()) \
        .offset((page - 1) * page_size) \
        .limit(page_size) \
        .all()
    
    return records, total


def get_daily(
    db: Session,
    start_date: date,
    end_date: date,
    datasource_id: int | None = None,
    page: int = 1,
    page_size: int = 100
) -> tuple[list[FactEnergyDaily], int]:
    """
    Get daily energy records with optional filters.
    
    Returns:
        Tuple of (records, total_count)
    """
    query = db.query(FactEnergyDaily).filter(
        FactEnergyDaily.ts >= start_date,
        FactEnergyDaily.ts <= end_date
    )
    
    if datasource_id:
        query = query.join(
            IngestBatch, FactEnergyDaily.source_batch_id == IngestBatch.batch_id
        ).filter(IngestBatch.datasource_id == datasource_id)
    
    total = query.count()
    
    records = query.order_by(FactEnergyDaily.ts.desc()) \
        .offset((page - 1) * page_size) \
        .limit(page_size) \
        .all()
    
    return records, total


def get_latest_hourly(db: Session, datasource_id: int | None = None) -> FactEnergyHourly | None:
    """Get the most recent hourly energy reading."""
    query = db.query(FactEnergyHourly)
    
    if datasource_id:
        query = query.join(
            IngestBatch, FactEnergyHourly.source_batch_id == IngestBatch.batch_id
        ).filter(IngestBatch.datasource_id == datasource_id)
    
    return query.order_by(FactEnergyHourly.ts.desc()).first()


def get_stats(db: Session) -> dict:
    """Get energy data statistics."""
    # Hourly stats
    hourly_stats = db.query(
        func.count(FactEnergyHourly.energy_id).label('count'),
        func.min(FactEnergyHourly.ts).label('first'),
        func.max(FactEnergyHourly.ts).label('last'),
        func.count(func.distinct(FactEnergyHourly.source_device_id)).label('devices')
    ).first()
    
    # Daily stats
    daily_stats = db.query(
        func.count(FactEnergyDaily.energy_id).label('count'),
        func.min(FactEnergyDaily.ts).label('first'),
        func.max(FactEnergyDaily.ts).label('last'),
        func.count(func.distinct(FactEnergyDaily.source_device_id)).label('devices')
    ).first()
    
    return {
        "hourly_count": hourly_stats.count or 0,
        "daily_count": daily_stats.count or 0,
        "hourly_first": hourly_stats.first,
        "hourly_last": hourly_stats.last,
        "daily_first": daily_stats.first,
        "daily_last": daily_stats.last,
        "hourly_devices": hourly_stats.devices or 0,
        "daily_devices": daily_stats.devices or 0,
    }
