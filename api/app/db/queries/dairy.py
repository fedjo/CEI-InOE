"""Dairy production queries."""

from datetime import date
from sqlalchemy import func
from sqlalchemy.orm import Session

from shared import DairyProduction


def get_production(
    db: Session,
    start_date: date | None = None,
    end_date: date | None = None,
    datasource_id: int | None = None,
    page: int = 1,
    page_size: int = 100
) -> tuple[list[DairyProduction], int]:
    """
    Get dairy production records with optional filters.
    
    Returns:
        Tuple of (records, total_count)
    """
    query = db.query(DairyProduction)
    
    if start_date:
        query = query.filter(DairyProduction.production_date >= start_date)
    
    if end_date:
        query = query.filter(DairyProduction.production_date <= end_date)

    if datasource_id is not None:
        query = query.filter(DairyProduction.datasource_id == datasource_id)
    
    total = query.count()
    
    records = query.order_by(DairyProduction.production_date.desc()) \
        .offset((page - 1) * page_size) \
        .limit(page_size) \
        .all()
    
    return records, total


def get_latest(db: Session, datasource_id: int | None = None) -> DairyProduction | None:
    """Get the most recent dairy production record."""
    query = db.query(DairyProduction)

    if datasource_id is not None:
        query = query.filter(DairyProduction.datasource_id == datasource_id)

    return query \
        .order_by(DairyProduction.production_date.desc()) \
        .first()


def get_stats(db: Session) -> dict:
    """Get dairy production statistics."""
    stats = db.query(
        func.count(DairyProduction.id).label('count'),
        func.min(DairyProduction.production_date).label('first'),
        func.max(DairyProduction.production_date).label('last'),
        func.avg(DairyProduction.day_production_per_cow_kg).label('avg_production'),
        func.avg(DairyProduction.number_of_animals).label('avg_animals'),
        func.avg(DairyProduction.feed_efficiency).label('avg_efficiency'),
    ).first()
    
    return {
        "total_count": stats.count or 0,
        "first_record": stats.first,
        "last_record": stats.last,
        "avg_production_per_cow": float(stats.avg_production) if stats.avg_production else None,
        "avg_animals": int(stats.avg_animals) if stats.avg_animals else None,
        "avg_feed_efficiency": float(stats.avg_efficiency) if stats.avg_efficiency else None,
    }
