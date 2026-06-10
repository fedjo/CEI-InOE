"""Environmental metrics queries."""

from datetime import date, datetime
from sqlalchemy import func
from sqlalchemy.orm import Session

from shared import EnvironmentalMetrics


def get_metrics(
    db: Session,
    start_date: date,
    end_date: date,
    source_device_id: str | None = None,
    datasource_id: int | None = None,
    page: int = 1,
    page_size: int = 100
) -> tuple[list[EnvironmentalMetrics], int]:
    """
    Get environmental metrics with optional filters.
    
    Returns:
        Tuple of (records, total_count)
    """
    # Convert dates to datetime for comparison
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())
    
    query = db.query(EnvironmentalMetrics).filter(
        EnvironmentalMetrics.timestamp >= start_dt,
        EnvironmentalMetrics.timestamp <= end_dt
    )
    
    if source_device_id:
        query = query.filter(EnvironmentalMetrics.source_device_id == source_device_id)

    if datasource_id is not None:
        query = query.filter(EnvironmentalMetrics.datasource_id == datasource_id)
    
    total = query.count()
    
    records = query.order_by(EnvironmentalMetrics.timestamp.desc()) \
        .offset((page - 1) * page_size) \
        .limit(page_size) \
        .all()
    
    return records, total


def get_latest(db: Session, datasource_id: int | None = None) -> EnvironmentalMetrics | None:
    """Get the most recent environmental reading."""
    query = db.query(EnvironmentalMetrics)

    if datasource_id is not None:
        query = query.filter(EnvironmentalMetrics.datasource_id == datasource_id)

    return query \
        .order_by(EnvironmentalMetrics.timestamp.desc()) \
        .first()


def get_stats(db: Session) -> dict:
    """Get environmental data statistics."""
    stats = db.query(
        func.count(EnvironmentalMetrics.id).label('count'),
        func.min(EnvironmentalMetrics.timestamp).label('first'),
        func.max(EnvironmentalMetrics.timestamp).label('last'),
        func.avg(EnvironmentalMetrics.temperature).label('avg_temp'),
        func.avg(EnvironmentalMetrics.humidity).label('avg_humidity'),
        func.avg(EnvironmentalMetrics.pm10).label('avg_pm10'),
        func.avg(EnvironmentalMetrics.pm2p5).label('avg_pm2p5'),
        func.count(func.distinct(func.date(EnvironmentalMetrics.timestamp))).label('days')
    ).first()
    
    return {
        "total_count": stats.count or 0,
        "first_record": stats.first,
        "last_record": stats.last,
        "avg_temperature": float(stats.avg_temp) if stats.avg_temp else None,
        "avg_humidity": float(stats.avg_humidity) if stats.avg_humidity else None,
        "avg_pm10": float(stats.avg_pm10) if stats.avg_pm10 else None,
        "avg_pm2p5": float(stats.avg_pm2p5) if stats.avg_pm2p5 else None,
        "days_with_data": stats.days or 0,
    }
