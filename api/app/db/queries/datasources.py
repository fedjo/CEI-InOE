"""
Datasource queries using SQLAlchemy ORM.

Replaces the old devices.py queries.
"""

from sqlalchemy import func
from sqlalchemy.orm import Session

from shared import Datasource


def get_datasources(
    db: Session,
    data_type: str | None = None,
    source_category: str | None = None,
    status: str | None = None,
    page: int = 1,
    page_size: int = 100
) -> tuple[list[Datasource], int]:
    """
    Get datasources with optional filters.
    
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
