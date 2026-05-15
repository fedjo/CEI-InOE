"""Energy data endpoints."""

from fastapi import APIRouter, Query, HTTPException, Depends
from datetime import date
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.db.queries import energy as energy_queries
from app.config import settings

from shared import EnergyHourlyRead, EnergyDailyRead, PaginatedResponse

router = APIRouter()


@router.get("/hourly", response_model=PaginatedResponse)
async def get_hourly_energy(
    start_date: date = Query(..., description="Start date (inclusive)"),
    end_date: date = Query(..., description="End date (inclusive)"),
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
    Get hourly energy consumption records.
    
    Returns paginated hourly energy data.
    """
    if start_date > end_date:
        raise HTTPException(
            status_code=400, 
            detail="start_date must be before or equal to end_date"
        )
    
    records, total = energy_queries.get_hourly(
        db=db,
        start_date=start_date,
        end_date=end_date,
        datasource_id=datasource_id,
        page=page,
        page_size=page_size
    )
    
    return PaginatedResponse(
        data=[EnergyHourlyRead.model_validate(r) for r in records],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=(total + page_size - 1) // page_size
    )


@router.get("/daily", response_model=PaginatedResponse)
async def get_daily_energy(
    start_date: date = Query(..., description="Start date (inclusive)"),
    end_date: date = Query(..., description="End date (inclusive)"),
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
    Get daily energy consumption records.
    
    Returns paginated daily energy data.
    """
    if start_date > end_date:
        raise HTTPException(
            status_code=400, 
            detail="start_date must be before or equal to end_date"
        )
    
    records, total = energy_queries.get_daily(
        db=db,
        start_date=start_date,
        end_date=end_date,
        datasource_id=datasource_id,
        page=page,
        page_size=page_size
    )
    
    return PaginatedResponse(
        data=[EnergyDailyRead.model_validate(r) for r in records],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=(total + page_size - 1) // page_size
    )


@router.get("/latest", response_model=EnergyHourlyRead)
async def get_latest_energy(
    datasource_id: int | None = Query(None, description="Filter by datasource ID"),
    db: Session = Depends(get_db),
):
    """
    Get the most recent hourly energy reading.
    """
    record = energy_queries.get_latest_hourly(db, datasource_id)
    
    if not record:
        raise HTTPException(status_code=404, detail="No energy data found")
    
    return EnergyHourlyRead.model_validate(record)


@router.get("/stats")
async def get_energy_stats(db: Session = Depends(get_db)):
    """
    Get energy data statistics.
    
    Returns counts, date ranges, and datasource counts.
    """
    return energy_queries.get_stats(db)
