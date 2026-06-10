"""Site endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.config import settings
from app.db.queries import datasources as ds_queries
from app.db.queries import sites as site_queries
from app.db.session import get_db
from shared import DatasourceRead, PaginatedResponse, SiteRead

router = APIRouter()


@router.get("", response_model=list[SiteRead])
async def get_sites(db: Session = Depends(get_db)):
    """Get all available sites."""
    sites = site_queries.get_sites(db)
    return [SiteRead.model_validate(site) for site in sites]


@router.get("/{site_id}/datasources", response_model=PaginatedResponse)
async def get_site_datasources(
    site_id: int,
    data_type: str | None = Query(None, description="Filter by data type (energy, weather, dairy, pv)"),
    source_category: str | None = Query(None, description="Filter by source category (device, file, api)"),
    status: str | None = Query(None, description="Filter by status"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(
        settings.default_page_size,
        ge=1,
        le=settings.max_page_size,
        description="Records per page",
    ),
    db: Session = Depends(get_db),
):
    """Get datasources for a specific site."""
    site = site_queries.get_site_by_id(db, site_id)

    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    datasources, total = ds_queries.get_datasources(
        db=db,
        data_type=data_type,
        source_category=source_category,
        status=status,
        site_id=site_id,
        page=page,
        page_size=page_size,
    )

    return PaginatedResponse(
        data=[DatasourceRead.model_validate(ds) for ds in datasources],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=(total + page_size - 1) // page_size,
    )