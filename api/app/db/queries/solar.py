"""Solar/PV queries."""

from datetime import date, datetime
from sqlalchemy import func
from sqlalchemy.orm import Session

from shared import FactSolarHourly, FactSolarDaily, FactSolarMonthly


def get_hourly(
    db: Session,
    start_date: date,
    end_date: date,
    datasource_id: int | None = None,
    page: int = 1,
    page_size: int = 100
) -> tuple[list[FactSolarHourly], int]:
    """
    Get hourly solar records with optional filters.
    
    Returns:
        Tuple of (records, total_count)
    """
    # Convert dates to datetime for comparison
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())
    
    query = db.query(FactSolarHourly).filter(
        FactSolarHourly.ts >= start_dt,
        FactSolarHourly.ts <= end_dt
    )
    
    if datasource_id:
        query = query.filter(FactSolarHourly.datasource_id == datasource_id)
    
    total = query.count()
    
    records = query.order_by(FactSolarHourly.ts.desc()) \
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
) -> tuple[list[FactSolarDaily], int]:
    """
    Get daily solar records with optional filters.
    
    Returns:
        Tuple of (records, total_count)
    """
    query = db.query(FactSolarDaily).filter(
        FactSolarDaily.ts >= start_date,
        FactSolarDaily.ts <= end_date
    )
    
    if datasource_id:
        query = query.filter(FactSolarDaily.datasource_id == datasource_id)
    
    total = query.count()
    
    records = query.order_by(FactSolarDaily.ts.desc()) \
        .offset((page - 1) * page_size) \
        .limit(page_size) \
        .all()
    
    return records, total


def get_monthly(
    db: Session,
    start_date: date,
    end_date: date,
    datasource_id: int | None = None,
    page: int = 1,
    page_size: int = 100
) -> tuple[list[FactSolarMonthly], int]:
    """
    Get monthly solar records with optional filters.
    
    Returns:
        Tuple of (records, total_count)
    """
    query = db.query(FactSolarMonthly).filter(
        FactSolarMonthly.ts >= start_date,
        FactSolarMonthly.ts <= end_date
    )
    
    if datasource_id:
        query = query.filter(FactSolarMonthly.datasource_id == datasource_id)
    
    total = query.count()
    
    records = query.order_by(FactSolarMonthly.ts.desc()) \
        .offset((page - 1) * page_size) \
        .limit(page_size) \
        .all()
    
    return records, total


def get_latest_hourly(db: Session, datasource_id: int | None = None) -> FactSolarHourly | None:
    """Get the most recent hourly solar reading."""
    query = db.query(FactSolarHourly)
    
    if datasource_id:
        query = query.filter(FactSolarHourly.datasource_id == datasource_id)
    
    return query.order_by(FactSolarHourly.ts.desc()).first()


def get_stats(db: Session) -> dict:
    """Get solar data statistics."""
    # Hourly stats
    hourly_stats = db.query(
        func.count(FactSolarHourly.id).label('count'),
        func.min(FactSolarHourly.ts).label('first'),
        func.max(FactSolarHourly.ts).label('last'),
        func.count(func.distinct(FactSolarHourly.datasource_id)).label('datasources')
    ).first()
    
    # Daily stats
    daily_stats = db.query(
        func.count(FactSolarDaily.id).label('count'),
        func.min(FactSolarDaily.ts).label('first'),
        func.max(FactSolarDaily.ts).label('last'),
        func.count(func.distinct(FactSolarDaily.datasource_id)).label('datasources')
    ).first()
    
    # Monthly stats
    monthly_stats = db.query(
        func.count(FactSolarMonthly.id).label('count'),
        func.min(FactSolarMonthly.ts).label('first'),
        func.max(FactSolarMonthly.ts).label('last'),
        func.count(func.distinct(FactSolarMonthly.datasource_id)).label('datasources')
    ).first()
    
    return {
        "hourly_count": hourly_stats.count or 0,
        "daily_count": daily_stats.count or 0,
        "monthly_count": monthly_stats.count or 0,
        "hourly_first": hourly_stats.first,
        "hourly_last": hourly_stats.last,
        "daily_first": daily_stats.first,
        "daily_last": daily_stats.last,
        "monthly_first": monthly_stats.first,
        "monthly_last": monthly_stats.last,
        "hourly_datasources": hourly_stats.datasources or 0,
        "daily_datasources": daily_stats.datasources or 0,
        "monthly_datasources": monthly_stats.datasources or 0,
    }
