"""Environmental metrics endpoints."""

from fastapi import APIRouter, Query, HTTPException, Depends
from datetime import date
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.db.queries import environmental as env_queries
from app.config import settings

from shared import EnvironmentalMetricsRead, PaginatedResponse

router = APIRouter()


@router.get("/hourly", response_model=PaginatedResponse)
async def get_environmental_metrics(
    start_date: date = Query(..., description="Start date (inclusive)"),
    end_date: date = Query(..., description="End date (inclusive)"),
    source_device_id: str | None = Query(None, description="Filter by source device ID"),
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
    Get environmental metrics records.
    
    Returns paginated environmental data.
    """
    if start_date > end_date:
        raise HTTPException(
            status_code=400, 
            detail="start_date must be before or equal to end_date"
        )
    
    records, total = env_queries.get_metrics(
        db=db,
        start_date=start_date,
        end_date=end_date,
        source_device_id=source_device_id,
        page=page,
        page_size=page_size
    )
    
    return PaginatedResponse(
        data=[EnvironmentalMetricsRead.model_validate(r) for r in records],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=(total + page_size - 1) // page_size
    )


@router.get("/latest", response_model=EnvironmentalMetricsRead)
async def get_latest_environmental(db: Session = Depends(get_db)):
    """
    Get the most recent environmental reading.
    """
    record = env_queries.get_latest(db)
    
    if not record:
        raise HTTPException(status_code=404, detail="No environmental data found")
    
    return EnvironmentalMetricsRead.model_validate(record)


@router.get("/stats")
async def get_environmental_stats(db: Session = Depends(get_db)):
    """
    Get environmental data statistics.
    
    Returns counts, date ranges, and averages.
    """
    return env_queries.get_stats(db)
