"""Dairy production schemas."""

from pydantic import BaseModel, ConfigDict
from datetime import datetime, date
from uuid import UUID


class DairyProductionRecord(BaseModel):
    """Dairy production record."""
    
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    production_date: date
    day_production_per_cow_kg: float | None = None
    number_of_animals: int | None = None
    average_lactation_days: int | None = None
    fed_per_cow_total_kg: float | None = None
    fed_per_cow_water_kg: float | None = None
    feed_efficiency: float | None = None
    rumination_minutes: int | None = None
    source_type: str | None = None
    source_file: UUID | None = None
    ingested_at: datetime | None = None


class DairyLatestRecord(BaseModel):
    """Latest dairy production record (simplified)."""
    
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    production_date: date
    day_production_per_cow_kg: float | None = None
    number_of_animals: int | None = None
    average_lactation_days: int | None = None
    fed_per_cow_total_kg: float | None = None
    fed_per_cow_water_kg: float | None = None
    feed_efficiency: float | None = None
    rumination_minutes: int | None = None


class DairyStatsResponse(BaseModel):
    """Dairy statistics response."""
    
    model_config = ConfigDict(from_attributes=True)
    
    total_count: int = 0
    first_record: date | None = None
    last_record: date | None = None
    avg_production_per_cow: float | None = None
    avg_animals: int | None = None
    avg_feed_efficiency: float | None = None
    avg_rumination_minutes: int | None = None
