"""Dairy production endpoints."""

from fastapi import APIRouter, Query, HTTPException, Depends
from datetime import date
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.db.queries import dairy as dairy_queries
from app.config import settings
from app.auth import AuthenticatedPrincipal, ensure_datasource_access, verify_api_key

from shared import DairyProductionRead, PaginatedResponse

router = APIRouter()


@router.get("/daily", response_model=PaginatedResponse)
async def get_dairy_production(
    start_date: date | None = Query(None, description="Start date (inclusive)"),
    end_date: date | None = Query(None, description="End date (inclusive)"),
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
    Get dairy production records.
    
    Returns paginated dairy data.
    """
    ensure_datasource_access(principal, datasource_id)

    if start_date and end_date and start_date > end_date:
        raise HTTPException(
            status_code=400, 
            detail="start_date must be before or equal to end_date"
        )
    
    records, total = dairy_queries.get_production(
        db=db,
        start_date=start_date,
        end_date=end_date,
        datasource_id=datasource_id,
        page=page,
        page_size=page_size
    )
    
    return PaginatedResponse(
        data=[DairyProductionRead.model_validate(r) for r in records],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=(total + page_size - 1) // page_size
    )


@router.get("/latest", response_model=DairyProductionRead)
async def get_latest_dairy(
    datasource_id: int = Query(..., description="Filter by datasource ID"),
    db: Session = Depends(get_db),
    principal: AuthenticatedPrincipal = Depends(verify_api_key),
):
    """
    Get the most recent dairy production record.
    """
    ensure_datasource_access(principal, datasource_id)

    record = dairy_queries.get_latest(db, datasource_id)
    
    if not record:
        raise HTTPException(status_code=404, detail="No dairy data found")
    
    return DairyProductionRead.model_validate(record)


@router.get("/stats")
async def get_dairy_stats(
    db: Session = Depends(get_db),
    principal: AuthenticatedPrincipal = Depends(verify_api_key),
):
    """
    Get dairy data statistics.
    
    Returns counts, date ranges, and averages.
    """
    if not principal.is_superuser:
        raise HTTPException(status_code=403, detail="Not available for restricted principals")

    return dairy_queries.get_stats(db)
