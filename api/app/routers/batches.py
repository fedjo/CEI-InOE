"""
Ingest batch endpoints.

Provides visibility into data ingestion operations.
"""

from fastapi import APIRouter, Query, HTTPException, Depends
from sqlalchemy.orm import Session
from uuid import UUID

from app.db.session import get_db
from app.db.queries import batches as batch_queries
from app.config import settings

from shared import IngestBatchRead, IngestBatchSummary, PaginatedResponse

router = APIRouter()


@router.get("", response_model=PaginatedResponse)
async def get_batches(
    source_type: str | None = Query(None, description="Filter by source type (file, api, stream)"),
    status: str | None = Query(None, description="Filter by status (pending, completed, failed)"),
    datasource_id: int | None = Query(None, description="Filter by datasource ID"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(
        settings.default_page_size, 
        ge=1, 
        le=settings.max_page_size,
        description="Records per page"
    ),
    db: Session = Depends(get_db),
):
    """
    Get ingest batches with optional filters.
    
    Batches represent units of data ingestion (file uploads, API fetches, etc.)
    """
    batches, total = batch_queries.get_batches(
        db=db,
        source_type=source_type,
        status=status,
        datasource_id=datasource_id,
        page=page,
        page_size=page_size
    )
    
    return PaginatedResponse(
        data=[IngestBatchRead.model_validate(b) for b in batches],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=(total + page_size - 1) // page_size
    )


@router.get("/summary", response_model=IngestBatchSummary)
async def get_batch_summary(db: Session = Depends(get_db)):
    """
    Get summary statistics for ingest batches.
    
    Returns total counts, success/failure rates, and record counts.
    """
    return batch_queries.get_batch_summary(db)


@router.get("/{batch_id}", response_model=IngestBatchRead)
async def get_batch(
    batch_id: UUID,
    db: Session = Depends(get_db),
):
    """
    Get a specific ingest batch by ID.
    """
    batch = batch_queries.get_batch_by_id(db, batch_id)
    
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")
    
    return IngestBatchRead.model_validate(batch)
