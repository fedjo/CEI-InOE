"""Device endpoints."""

from fastapi import APIRouter, Query, HTTPException
from typing import List

from app.db.queries import devices as device_queries
from app.schemas.common import PaginatedResponse
from app.schemas.devices import DeviceRecord, DeviceTypeCount
from app.config import settings

router = APIRouter()


@router.get("", response_model=PaginatedResponse[DeviceRecord])
async def get_devices(
    device_type: str | None = Query(None, description="Filter by device type"),
    status: str | None = Query(None, description="Filter by status"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(
        settings.default_page_size, 
        ge=1, 
        le=settings.max_page_size,
        description="Records per page"
    ),
):
    """
    Get devices.
    
    Returns paginated devices from generic_device table.
    """
    rows, total = device_queries.get_devices(
        device_type=device_type,
        status=status,
        page=page,
        page_size=page_size
    )
    
    return PaginatedResponse.create(
        data=[DeviceRecord(**row) for row in rows],
        total=total,
        page=page,
        page_size=page_size
    )


@router.get("/types", response_model=List[DeviceTypeCount])
async def get_device_types():
    """
    Get device types with counts.
    
    Returns a list of unique device types and how many devices of each type exist.
    """
    rows = device_queries.get_device_types()
    return [DeviceTypeCount(**row) for row in rows]


@router.get("/{device_id}", response_model=DeviceRecord)
async def get_device(device_id: int):
    """
    Get a specific device by ID.
    """
    result = device_queries.get_device_by_id(device_id)
    
    if not result:
        raise HTTPException(status_code=404, detail="Device not found")
    
    return DeviceRecord(**result)
