"""Environmental metrics endpoints."""

from fastapi import APIRouter, Query, HTTPException
from datetime import date

from app.db.queries import environmental as env_queries
from app.schemas.common import PaginatedResponse
from app.schemas.environmental import (
    EnvironmentalMetricRecord, 
    EnvironmentalLatestRecord,
    EnvironmentalStatsResponse
)
from app.config import settings

router = APIRouter()


@router.get("/hourly", response_model=PaginatedResponse[EnvironmentalMetricRecord])
async def get_environmental_metrics(
    start_date: date = Query(..., description="Start date (inclusive)"),
    end_date: date = Query(..., description="End date (inclusive)"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(
        settings.default_page_size, 
        ge=1, 
        le=settings.max_page_size,
        description="Records per page"
    ),
):
    """
    Get environmental metrics records.
    
    Returns paginated environmental data from environmental_metrics table.
    """
    if start_date > end_date:
        raise HTTPException(
            status_code=400, 
            detail="start_date must be before or equal to end_date"
        )
    
    rows, total = env_queries.get_metrics(
        start_date=start_date,
        end_date=end_date,
        page=page,
        page_size=page_size
    )
    
    return PaginatedResponse.create(
        data=[EnvironmentalMetricRecord(**row) for row in rows],
        total=total,
        page=page,
        page_size=page_size
    )


@router.get("/latest", response_model=EnvironmentalLatestRecord)
async def get_latest_environmental():
    """
    Get the most recent environmental reading.
    """
    result = env_queries.get_latest()
    
    if not result:
        raise HTTPException(status_code=404, detail="No environmental data found")
    
    return EnvironmentalLatestRecord(**result)


@router.get("/stats", response_model=EnvironmentalStatsResponse)
async def get_environmental_stats():
    """
    Get environmental data statistics.
    
    Returns counts, date ranges, and averages for environmental data.
    """
    result = env_queries.get_stats()
    return EnvironmentalStatsResponse(**result)
