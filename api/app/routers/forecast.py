"""Weather forecast endpoints."""

from datetime import datetime
from fastapi import APIRouter, Query, HTTPException, Depends
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.db.queries import forecast as forecast_queries
from app.config import settings

from shared import WeatherForecastRead, PaginatedResponse

router = APIRouter()


@router.get("/latest", response_model=list[WeatherForecastRead])
async def get_latest_forecast(
    site_id: int | None = Query(1, description="Filter by site ID"),
    db: Session = Depends(get_db),
):
    """
    Return all hourly slots from the most recent forecast run.

    Useful for displaying the current 7-day forecast on a dashboard.
    """
    records = forecast_queries.get_latest_forecast(db=db, site_id=site_id)

    if not records:
        raise HTTPException(status_code=404, detail="No forecast data found")

    return [WeatherForecastRead.model_validate(r) for r in records]


@router.get("/history", response_model=PaginatedResponse)
async def get_forecast_history(
    valid_at_start: datetime = Query(..., description="Start of valid-time window (inclusive)"),
    valid_at_end: datetime = Query(..., description="End of valid-time window (inclusive)"),
    site_id: int | None = Query(1, description="Filter by site ID"),
    forecast_run_at: datetime | None = Query(
        None, description="Restrict to a specific forecast run timestamp"
    ),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(
        settings.default_page_size,
        ge=1,
        le=settings.max_page_size,
        description="Records per page",
    ),
    db: Session = Depends(get_db),
):
    """
    Query forecast history by valid-time range.

    Useful for overlaying past forecasts vs. actual measurements in Grafana.
    Supports filtering by a specific forecast run to compare different runs
    for the same valid-time window.
    """
    if valid_at_start > valid_at_end:
        raise HTTPException(
            status_code=400,
            detail="valid_at_start must be before or equal to valid_at_end",
        )

    records, total = forecast_queries.get_forecast_history(
        db=db,
        valid_at_start=valid_at_start,
        valid_at_end=valid_at_end,
        site_id=site_id,
        forecast_run_at=forecast_run_at,
        page=page,
        page_size=page_size,
    )

    return PaginatedResponse(
        data=[WeatherForecastRead.model_validate(r) for r in records],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=(total + page_size - 1) // page_size,
    )
