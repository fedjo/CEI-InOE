"""
Datasource endpoints (replaces devices.py).

Uses SQLAlchemy ORM for queries.
"""

from fastapi import APIRouter, Query, HTTPException, Depends
from sqlalchemy.orm import Session
from typing import List

from app.db.session import get_db
from app.db.queries import datasources as ds_queries
from app.config import settings

from shared import (
    DatasourceRead, 
    DatasourceCreate, 
    DatasourceUpdate,
    DatasourceTypeCount,
    PaginatedResponse,
)

router = APIRouter()


@router.get("", response_model=PaginatedResponse)
async def get_datasources(
    data_type: str | None = Query(None, description="Filter by data type (energy, weather, dairy, pv)"),
    source_category: str | None = Query(None, description="Filter by source category (device, file, api)"),
    status: str | None = Query(None, description="Filter by status"),
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
    Get datasources with optional filters.
    
    Datasources are the unified registry for data sources including devices, files, and APIs.
    """
    datasources, total = ds_queries.get_datasources(
        db=db,
        data_type=data_type,
        source_category=source_category,
        status=status,
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
async def get_data_types(db: Session = Depends(get_db)):
    """
    Get data types with counts.
    
    Returns a list of unique data types and how many datasources of each type exist.
    """
    return ds_queries.get_data_types(db)


@router.get("/categories")
async def get_source_categories(db: Session = Depends(get_db)):
    """
    Get source categories with counts.
    
    Returns a list of unique source categories (device, file, api, manual).
    """
    return ds_queries.get_source_categories(db)


@router.get("/{datasource_id}", response_model=DatasourceRead)
async def get_datasource(
    datasource_id: int,
    db: Session = Depends(get_db),
):
    """
    Get a specific datasource by ID.
    """
    datasource = ds_queries.get_datasource_by_id(db, datasource_id)
    
    if not datasource:
        raise HTTPException(status_code=404, detail="Datasource not found")
    
    return DatasourceRead.model_validate(datasource)


@router.get("/external/{external_id}", response_model=DatasourceRead)
async def get_datasource_by_external_id(
    external_id: str,
    db: Session = Depends(get_db),
):
    """
    Get a datasource by its external ID.
    """
    datasource = ds_queries.get_datasource_by_external_id(db, external_id)
    
    if not datasource:
        raise HTTPException(status_code=404, detail="Datasource not found")
    
    return DatasourceRead.model_validate(datasource)
