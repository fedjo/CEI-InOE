"""
Datasource endpoints (replaces devices.py).

Uses SQLAlchemy ORM for queries.
"""

from datetime import datetime, timezone

from fastapi import APIRouter, Query, HTTPException, Depends
from fastapi import status as http_status
from sqlalchemy.orm import Session
from typing import List

from app.db.session import get_db
from app.db.queries import datasources as ds_queries
from app.config import settings
from app.auth import AuthenticatedPrincipal, ensure_datasource_access, require_superuser, verify_api_key

from shared import (
    Datasource,
    DatasourceCreate,
    DatasourceRead,
    DatasourceUpdate,
    DatasourceTypeCount,
    PaginatedResponse,
    PurgeResult,
)

router = APIRouter()


@router.get("", response_model=PaginatedResponse)
async def get_datasources(
    data_type: str | None = Query(None, description="Filter by data type (energy, weather, dairy, pv)"),
    source_category: str | None = Query(None, description="Filter by source category (device, file, api)"),
    status: str | None = Query(None, description="Filter by status"),
    site_id: int | None = Query(None, description="Filter by site ID"),
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
    Get datasources with optional filters.
    
    Datasources are the unified registry for data sources including devices, files, and APIs.
    Restricted principals only see datasources they've been granted access to.
    """
    datasource_ids = None if principal.is_superuser else list(principal.allowed_datasource_ids)
    if datasource_ids is not None and not datasource_ids:
        return PaginatedResponse(data=[], total=0, page=page, page_size=page_size, total_pages=0)

    datasources, total = ds_queries.get_datasources(
        db=db,
        data_type=data_type,
        source_category=source_category,
        status=status,
        site_id=site_id,
        datasource_ids=datasource_ids,
        page=page,
        page_size=page_size
    )
    
    return PaginatedResponse(
        data=[DatasourceRead.model_validate(ds) for ds in datasources],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=(total + page_size - 1) // page_size
    )


@router.get("/types", response_model=List[DatasourceTypeCount])
async def get_data_types(
    db: Session = Depends(get_db),
    _: AuthenticatedPrincipal = Depends(require_superuser),
):
    """
    Get data types with counts.
    
    Returns a list of unique data types and how many datasources of each type exist.
    """
    return ds_queries.get_data_types(db)


@router.get("/categories")
async def get_source_categories(
    db: Session = Depends(get_db),
    _: AuthenticatedPrincipal = Depends(require_superuser),
):
    """
    Get source categories with counts.
    
    Returns a list of unique source categories (device, file, api, manual).
    """
    return ds_queries.get_source_categories(db)


@router.get("/external/{external_id}", response_model=DatasourceRead)
async def get_datasource_by_external_id(
    external_id: str,
    db: Session = Depends(get_db),
    principal: AuthenticatedPrincipal = Depends(verify_api_key),
):
    """
    Get a datasource by its external ID.
    """
    datasource = ds_queries.get_datasource_by_external_id(db, external_id)
    
    if not datasource:
        raise HTTPException(status_code=404, detail="Datasource not found")
    ensure_datasource_access(principal, datasource.id)
    
    return DatasourceRead.model_validate(datasource)


@router.get("/{datasource_id}", response_model=DatasourceRead)
async def get_datasource(
    datasource_id: int,
    db: Session = Depends(get_db),
    principal: AuthenticatedPrincipal = Depends(verify_api_key),
):
    """
    Get a specific datasource by ID.
    """
    ensure_datasource_access(principal, datasource_id)

    datasource = ds_queries.get_datasource_by_id(db, datasource_id)
    
    if not datasource:
        raise HTTPException(status_code=404, detail="Datasource not found")
    
    return DatasourceRead.model_validate(datasource)


# =============================================================================
# Write endpoints
# =============================================================================

@router.post("", response_model=DatasourceRead, status_code=http_status.HTTP_201_CREATED)
async def create_datasource(
    payload: DatasourceCreate,
    db: Session = Depends(get_db),
    _: AuthenticatedPrincipal = Depends(require_superuser),
):
    """
    Register a new datasource.

    The new datasource is created with the provided status (defaults to 'active').
    Set status='online' to make it immediately visible to ingestor connectors on
    their next discover cycle.
    """
    existing = ds_queries.get_datasource_by_external_id(db, payload.external_id)
    if existing:
        raise HTTPException(
            status_code=http_status.HTTP_409_CONFLICT,
            detail=f"Datasource with external_id '{payload.external_id}' already exists",
        )

    ds = Datasource(
        external_id=payload.external_id,
        source_category=payload.source_category,
        data_type=payload.data_type,
        name=payload.name,
        alias=payload.alias,
        client=payload.client,
        description=payload.description,
        status=payload.status,
        timezone=payload.timezone,
        metadata_=payload.metadata or {},
        site_id=payload.site_id,
    )
    created = ds_queries.create_datasource(db, ds)
    return DatasourceRead.model_validate(created)


@router.patch("/{datasource_id}", response_model=DatasourceRead)
async def update_datasource(
    datasource_id: int,
    payload: DatasourceUpdate,
    db: Session = Depends(get_db),
    _: AuthenticatedPrincipal = Depends(require_superuser),
):
    """
    Update mutable fields of a datasource (name, alias, description, status,
    timezone, metadata).

    Tip: PATCH with {"status": "online"} to activate a datasource so that
    ingestor connectors pick it up on the next discover cycle.
    """
    update_kwargs = payload.model_dump(exclude_none=True)
    if "metadata" in update_kwargs:
        update_kwargs["metadata_"] = update_kwargs.pop("metadata")

    datasource = ds_queries.update_datasource(db, datasource_id, **update_kwargs)
    if not datasource:
        raise HTTPException(status_code=404, detail="Datasource not found")
    return DatasourceRead.model_validate(datasource)


@router.delete("/{datasource_id}", response_model=DatasourceRead)
async def disable_datasource(
    datasource_id: int,
    db: Session = Depends(get_db),
    _: AuthenticatedPrincipal = Depends(require_superuser),
):
    """
    Soft-delete: set the datasource status to 'offline'.

    No data is removed. Ingestor connectors will stop fetching for this
    datasource on their next discover cycle. The datasource can be
    re-enabled with PATCH status='online'.
    """
    datasource = ds_queries.soft_delete_datasource(db, datasource_id)
    if not datasource:
        raise HTTPException(status_code=404, detail="Datasource not found")
    return DatasourceRead.model_validate(datasource)


@router.delete("/{datasource_id}/purge", response_model=PurgeResult)
async def purge_datasource(
    datasource_id: int,
    db: Session = Depends(get_db),
    _: AuthenticatedPrincipal = Depends(require_superuser),
):
    """
    Hard-delete: permanently remove the datasource and ALL its data.

    This deletes every related row across fact tables, ingest batches,
    API cursors, staging rows, and pipeline/quality records, then removes
    the datasource row itself.  This action is **irreversible**.
    """
    existing = ds_queries.get_datasource_by_id(db, datasource_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Datasource not found")

    rows_deleted = ds_queries.purge_datasource(db, datasource_id)
    return PurgeResult(
        datasource_id=datasource_id,
        rows_deleted=rows_deleted,
        purged_at=datetime.now(timezone.utc),
    )
