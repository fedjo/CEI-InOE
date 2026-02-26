"""Environmental metrics schemas."""

from pydantic import BaseModel, ConfigDict
from datetime import datetime
from uuid import UUID


class EnvironmentalMetricRecord(BaseModel):
    """Environmental metrics record."""
    
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    timestamp: datetime
    temperature: float | None = None
    humidity: float | None = None
    atm_pressure: float | None = None
    pm10: float | None = None
    pm2p5: float | None = None
    noise_level_db: float | None = None
    wind_speed: float | None = None
    wind_angle: float | None = None
    wind_direction_sectors: float | None = None
    source_device_id: str | None = None
    source_type: str | None = None
    source_file: UUID | None = None
    created_at: datetime | None = None


class EnvironmentalLatestRecord(BaseModel):
    """Latest environmental reading (simplified)."""
    
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    timestamp: datetime
    temperature: float | None = None
    humidity: float | None = None
    atm_pressure: float | None = None
    pm10: float | None = None
    pm2p5: float | None = None
    noise_level_db: float | None = None
    wind_speed: float | None = None
    wind_angle: float | None = None
    wind_direction_sectors: float | None = None


class EnvironmentalStatsResponse(BaseModel):
    """Environmental statistics response."""
    
    model_config = ConfigDict(from_attributes=True)
    
    total_count: int = 0
    first_record: datetime | None = None
    last_record: datetime | None = None
    avg_temperature: float | None = None
    avg_humidity: float | None = None
    avg_pm10: float | None = None
    avg_pm2p5: float | None = None
    days_with_data: int = 0
