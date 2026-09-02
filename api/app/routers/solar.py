"""Solar/PV data endpoints."""

from fastapi import APIRouter, Query, HTTPException, Depends
from datetime import date
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.db.queries import solar as solar_queries
from app.config import settings
from app.auth import AuthenticatedPrincipal, ensure_datasource_access, verify_api_key

from shared import SolarHourlyRead, SolarDailyRead, SolarMonthlyRead, PaginatedResponse

router = APIRouter()


@router.get("/hourly", response_model=PaginatedResponse)
async def get_hourly_solar(
    start_date: date = Query(..., description="Start date (inclusive)"),
    end_date: date = Query(..., description="End date (inclusive)"),
    datasource_id: int = Query(..., description="Filter by datasource ID"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(
        settings.default_page_size, 
        ge=1, 
        le=settings.max_page_size,
        description="Records per page"
    ),
    db: Session = Depends(get_db),
    principal: AuthenticatedPrincipal = Depends(verify_api_key),
):
    """
    Get hourly solar/PV generation records.
    
    Returns paginated hourly solar data.
    """
    ensure_datasource_access(principal, datasource_id)

    if start_date > end_date:
        raise HTTPException(
            status_code=400, 
            detail="start_date must be before or equal to end_date"
        )
    
    records, total = solar_queries.get_hourly(
        db=db,
        start_date=start_date,
        end_date=end_date,
        datasource_id=datasource_id,
        page=page,
        page_size=page_size
    )
    
    return PaginatedResponse(
        data=[SolarHourlyRead.model_validate(r) for r in records],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=(total + page_size - 1) // page_size
    )


@router.get("/daily", response_model=PaginatedResponse)
async def get_daily_solar(
    start_date: date = Query(..., description="Start date (inclusive)"),
    end_date: date = Query(..., description="End date (inclusive)"),
    datasource_id: int = Query(..., description="Filter by datasource ID"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(
        settings.default_page_size, 
        ge=1, 
        le=settings.max_page_size,
        description="Records per page"
    ),
    db: Session = Depends(get_db),
    principal: AuthenticatedPrincipal = Depends(verify_api_key),
):
    """
    Get daily solar/PV generation records.
    
    Returns paginated daily solar data.
    """
    ensure_datasource_access(principal, datasource_id)

    if start_date > end_date:
        raise HTTPException(
            status_code=400, 
            detail="start_date must be before or equal to end_date"
        )
    
    records, total = solar_queries.get_daily(
        db=db,
        start_date=start_date,
        end_date=end_date,
        datasource_id=datasource_id,
        page=page,
        page_size=page_size
    )
    
    return PaginatedResponse(
        data=[SolarDailyRead.model_validate(r) for r in records],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=(total + page_size - 1) // page_size
    )


@router.get("/monthly", response_model=PaginatedResponse)
async def get_monthly_solar(
    start_date: date = Query(..., description="Start date (inclusive)"),
    end_date: date = Query(..., description="End date (inclusive)"),
    datasource_id: int = Query(..., description="Filter by datasource ID"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(
        settings.default_page_size, 
        ge=1, 
        le=settings.max_page_size,
        description="Records per page"
    ),
    db: Session = Depends(get_db),
    principal: AuthenticatedPrincipal = Depends(verify_api_key),
):
    """
    Get monthly solar/PV generation records.
    
    Returns paginated monthly solar data.
    """
    ensure_datasource_access(principal, datasource_id)

    if start_date > end_date:
        raise HTTPException(
            status_code=400, 
            detail="start_date must be before or equal to end_date"
        )
    
    records, total = solar_queries.get_monthly(
        db=db,
        start_date=start_date,
        end_date=end_date,
        datasource_id=datasource_id,
        page=page,
        page_size=page_size
    )
    
    return PaginatedResponse(
        data=[SolarMonthlyRead.model_validate(r) for r in records],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=(total + page_size - 1) // page_size
    )


@router.get("/latest", response_model=SolarHourlyRead)
async def get_latest_solar(
    datasource_id: int = Query(..., description="Filter by datasource ID"),
    db: Session = Depends(get_db),
    principal: AuthenticatedPrincipal = Depends(verify_api_key),
):
    """
    Get the most recent hourly solar reading.
    """
    ensure_datasource_access(principal, datasource_id)

    record = solar_queries.get_latest_hourly(db, datasource_id)
    
    if not record:
        raise HTTPException(status_code=404, detail="No solar data found")
    
    return SolarHourlyRead.model_validate(record)


@router.get("/stats")
async def get_solar_stats(
    db: Session = Depends(get_db),
    principal: AuthenticatedPrincipal = Depends(verify_api_key),
):
    """
    Get solar data statistics.
    
    Returns counts, date ranges, and datasource counts.
    """
    if not principal.is_superuser:
        raise HTTPException(status_code=403, detail="Not available for restricted principals")

    return solar_queries.get_stats(db)
