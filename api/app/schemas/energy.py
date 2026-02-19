"""Energy data schemas."""

from pydantic import BaseModel, ConfigDict
from datetime import datetime, date
from uuid import UUID


class EnergyHourlyRecord(BaseModel):
    """Hourly energy consumption record."""
    
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    device_id: int
    ts: datetime
    kwh: float | None = None
    source_type: str | None = None
    source_file: UUID | None = None
    created_at: datetime | None = None


class EnergyDailyRecord(BaseModel):
    """Daily energy consumption record."""
    
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    device_id: int
    day: date
    kwh: float | None = None
    source_type: str | None = None
    source_file: UUID | None = None
    created_at: datetime | None = None


class EnergyStatsResponse(BaseModel):
    """Energy statistics response."""
    
    model_config = ConfigDict(from_attributes=True)
    
    hourly_count: int = 0
    daily_count: int = 0
    hourly_first: datetime | None = None
    hourly_last: datetime | None = None
    daily_first: date | None = None
    daily_last: date | None = None
    hourly_devices: int = 0
    daily_devices: int = 0
