"""Dairy production endpoints."""

from fastapi import APIRouter, Query, HTTPException
from datetime import date

from app.db.queries import dairy as dairy_queries
from app.schemas.common import PaginatedResponse
from app.schemas.dairy import (
    DairyProductionRecord, 
    DairyLatestRecord,
    DairyStatsResponse
)
from app.config import settings

router = APIRouter()


@router.get("/daily", response_model=PaginatedResponse[DairyProductionRecord])
async def get_dairy_production(
    start_date: date | None = Query(None, description="Start date (inclusive)"),
    end_date: date | None = Query(None, description="End date (inclusive)"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(
        settings.default_page_size, 
        ge=1, 
        le=settings.max_page_size,
        description="Records per page"
    ),
):
    """
    Get dairy production records.
    
    Returns paginated dairy data from dairy_production table.
    """
    if start_date and end_date and start_date > end_date:
        raise HTTPException(
            status_code=400, 
            detail="start_date must be before or equal to end_date"
        )
    
    rows, total = dairy_queries.get_production(
        start_date=start_date,
        end_date=end_date,
        page=page,
        page_size=page_size
    )
    
    return PaginatedResponse.create(
        data=[DairyProductionRecord(**row) for row in rows],
        total=total,
        page=page,
        page_size=page_size
    )


@router.get("/latest", response_model=DairyLatestRecord)
async def get_latest_dairy():
    """
    Get the most recent dairy production record.
    """
    result = dairy_queries.get_latest()
    
    if not result:
        raise HTTPException(status_code=404, detail="No dairy data found")
    
    return DairyLatestRecord(**result)


@router.get("/stats", response_model=DairyStatsResponse)
async def get_dairy_stats():
    """
    Get dairy data statistics.
    
    Returns counts, date ranges, and averages for dairy data.
    """
    result = dairy_queries.get_stats()
    return DairyStatsResponse(**result)
