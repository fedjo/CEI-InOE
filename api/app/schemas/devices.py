"""Device schemas."""

from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import Any


class DeviceRecord(BaseModel):
    """Device record."""
    
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    device_id: str
    device_type: str
    alias: str | None = None
    client: str
    description: str | None = None
    status: str | None = None
    timezone: str | None = None
    metadata: dict[str, Any] | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class DeviceTypeCount(BaseModel):
    """Device type with count."""
    
    device_type: str
    count: int
