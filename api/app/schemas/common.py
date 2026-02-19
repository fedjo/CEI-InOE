"""Common schemas used across the API."""

from pydantic import BaseModel, ConfigDict
from typing import Generic, TypeVar, List
from datetime import datetime

T = TypeVar('T')


class PaginatedResponse(BaseModel, Generic[T]):
    """Generic paginated response wrapper."""
    
    data: List[T]
    total: int
    page: int
    page_size: int
    total_pages: int
    
    @classmethod
    def create(
        cls, 
        data: List[T], 
        total: int, 
        page: int, 
        page_size: int
    ) -> "PaginatedResponse[T]":
        """Factory method to create paginated response."""
        total_pages = (total + page_size - 1) // page_size if page_size > 0 else 0
        return cls(
            data=data,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )


class HealthResponse(BaseModel):
    """Health check response."""
    
    status: str
    database: str
    timestamp: datetime


class StatsResponse(BaseModel):
    """Overall statistics response."""
    
    model_config = ConfigDict(from_attributes=True)
    
    environmental_count: int = 0
    energy_hourly_count: int = 0
    energy_daily_count: int = 0
    dairy_count: int = 0
    devices_count: int = 0
    latest_environmental: datetime | None = None
    latest_energy: datetime | None = None
    latest_dairy: datetime | None = None
