"""Energy data endpoints."""

from fastapi import APIRouter, Query, HTTPException
from datetime import date

from app.db.queries import energy as energy_queries
from app.schemas.common import PaginatedResponse
from app.schemas.energy import EnergyHourlyRecord, EnergyDailyRecord, EnergyStatsResponse
from app.config import settings

router = APIRouter()


@router.get("/hourly", response_model=PaginatedResponse[EnergyHourlyRecord])
async def get_hourly_energy(
    start_date: date = Query(..., description="Start date (inclusive)"),
    end_date: date = Query(..., description="End date (inclusive)"),
    device_id: int | None = Query(None, description="Filter by device ID"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(
        settings.default_page_size, 
        ge=1, 
        le=settings.max_page_size,
        description="Records per page"
    ),
):
    """
    Get hourly energy consumption records.
    
    Returns paginated hourly energy data from fact_energy_hourly table.
    """
    if start_date > end_date:
        raise HTTPException(
            status_code=400, 
            detail="start_date must be before or equal to end_date"
        )
    
    rows, total = energy_queries.get_hourly(
        start_date=start_date,
        end_date=end_date,
        device_id=device_id,
        page=page,
        page_size=page_size
    )
    
    return PaginatedResponse.create(
        data=[EnergyHourlyRecord(**row) for row in rows],
        total=total,
        page=page,
        page_size=page_size
    )


@router.get("/daily", response_model=PaginatedResponse[EnergyDailyRecord])
async def get_daily_energy(
    start_date: date = Query(..., description="Start date (inclusive)"),
    end_date: date = Query(..., description="End date (inclusive)"),
    device_id: int | None = Query(None, description="Filter by device ID"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(
        settings.default_page_size, 
        ge=1, 
        le=settings.max_page_size,
        description="Records per page"
    ),
):
    """
    Get daily energy consumption records.
    
    Returns paginated daily energy data from fact_energy_daily table.
    """
    if start_date > end_date:
        raise HTTPException(
            status_code=400, 
            detail="start_date must be before or equal to end_date"
        )
    
    rows, total = energy_queries.get_daily(
        start_date=start_date,
        end_date=end_date,
        device_id=device_id,
        page=page,
        page_size=page_size
    )
    
    return PaginatedResponse.create(
        data=[EnergyDailyRecord(**row) for row in rows],
        total=total,
        page=page,
        page_size=page_size
    )


@router.get("/latest", response_model=EnergyHourlyRecord)
async def get_latest_energy(
    device_id: int | None = Query(None, description="Filter by device ID"),
):
    """
    Get the most recent hourly energy reading.
    
    Optionally filter by device ID.
    """
    result = energy_queries.get_latest_hourly(device_id)
    
    if not result:
        raise HTTPException(status_code=404, detail="No energy data found")
    
    return EnergyHourlyRecord(**result)


@router.get("/stats", response_model=EnergyStatsResponse)
async def get_energy_stats():
    """
    Get energy data statistics.
    
    Returns counts, date ranges, and device counts for energy data.
    """
    result = energy_queries.get_stats()
    return EnergyStatsResponse(**result)
