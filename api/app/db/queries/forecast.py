"""Weather forecast queries."""

from datetime import datetime
from sqlalchemy import func
from sqlalchemy.orm import Session

from shared import FactWeatherForecast


def get_latest_forecast(
    db: Session,
    site_id: int | None = None,
) -> list[FactWeatherForecast]:
    """
    Return all hourly slots from the most recent forecast run.

    Args:
        db: Database session.
        site_id: Optional site filter (defaults to the first site if None).

    Returns:
        List of FactWeatherForecast records ordered by valid_at.
    """
    # Find the latest forecast_run_at for the requested site
    subq = db.query(func.max(FactWeatherForecast.forecast_run_at))
    if site_id is not None:
        subq = subq.filter(FactWeatherForecast.site_id == site_id)
    latest_run = subq.scalar()

    if latest_run is None:
        return []

    query = db.query(FactWeatherForecast).filter(
        FactWeatherForecast.forecast_run_at == latest_run
    )
    if site_id is not None:
        query = query.filter(FactWeatherForecast.site_id == site_id)

    return query.order_by(FactWeatherForecast.valid_at).all()


def get_forecast_history(
    db: Session,
    valid_at_start: datetime,
    valid_at_end: datetime,
    site_id: int | None = None,
    forecast_run_at: datetime | None = None,
    page: int = 1,
    page_size: int = 100,
) -> tuple[list[FactWeatherForecast], int]:
    """
    Query forecast records by valid_at range, optionally filtered by run time.

    Args:
        db: Database session.
        valid_at_start: Start of the valid-time window (inclusive).
        valid_at_end: End of the valid-time window (inclusive).
        site_id: Optional site filter.
        forecast_run_at: If given, restrict to a specific forecast run.
        page: 1-based page number.
        page_size: Records per page.

    Returns:
        Tuple of (records, total_count).
    """
    query = db.query(FactWeatherForecast).filter(
        FactWeatherForecast.valid_at >= valid_at_start,
        FactWeatherForecast.valid_at <= valid_at_end,
    )

    if site_id is not None:
        query = query.filter(FactWeatherForecast.site_id == site_id)

    if forecast_run_at is not None:
        query = query.filter(FactWeatherForecast.forecast_run_at == forecast_run_at)

    total = query.count()

    records = (
        query.order_by(FactWeatherForecast.forecast_run_at, FactWeatherForecast.valid_at)
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return records, total
