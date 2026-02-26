"""
Ingest batch queries using SQLAlchemy ORM.
"""

from uuid import UUID
from sqlalchemy import func
from sqlalchemy.orm import Session

from shared import IngestBatch


def get_batches(
    db: Session,
    source_type: str | None = None,
    status: str | None = None,
    datasource_id: int | None = None,
    page: int = 1,
    page_size: int = 100
) -> tuple[list[IngestBatch], int]:
    """
    Get ingest batches with optional filters.
    
    Returns:
        Tuple of (batches, total_count)
    """
    query = db.query(IngestBatch)
    
    if source_type:
        query = query.filter(IngestBatch.source_type == source_type)
    
    if status:
        query = query.filter(IngestBatch.status == status)
    
    if datasource_id:
        query = query.filter(IngestBatch.datasource_id == datasource_id)
    
    total = query.count()
    
    batches = query.order_by(IngestBatch.started_at.desc()) \
        .offset((page - 1) * page_size) \
        .limit(page_size) \
        .all()
    
    return batches, total


def get_batch_by_id(db: Session, batch_id: UUID) -> IngestBatch | None:
    """Get ingest batch by ID."""
    return db.query(IngestBatch).filter(IngestBatch.batch_id == batch_id).first()


def get_batch_summary(db: Session) -> dict:
    """Get summary statistics for ingest batches."""
    total = db.query(func.count(IngestBatch.batch_id)).scalar() or 0
    
    completed = db.query(func.count(IngestBatch.batch_id)) \
        .filter(IngestBatch.status == 'completed').scalar() or 0
    
    failed = db.query(func.count(IngestBatch.batch_id)) \
        .filter(IngestBatch.status == 'failed').scalar() or 0
    
    records_loaded = db.query(func.sum(IngestBatch.records_loaded)).scalar() or 0
    records_failed = db.query(func.sum(IngestBatch.records_failed)).scalar() or 0
    
    return {
        "total_batches": total,
        "completed": completed,
        "failed": failed,
        "total_records_loaded": int(records_loaded),
        "total_records_failed": int(records_failed),
    }
