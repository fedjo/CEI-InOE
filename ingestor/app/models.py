"""
Pydantic Models for CEI-InOE Data Warehouse

Defines strongly-typed models for all dataset types with:
- Field validators preserving existing coercion logic (European decimals, AM/PM dates)
- Constraint validation (min/max ranges)
- Source metadata tracking
"""

from typing import Optional, Dict, Any
from datetime import datetime, date
from uuid import UUID
from enum import Enum
import math

from pydantic import BaseModel, Field, field_validator, ConfigDict


class SourceType(str, Enum):
    """Data source type enumeration."""
    CSV = "csv"
    API = "api"
    EXCEL = "excel"
    UNKNOWN = "unknown"


# ══════════════════════════════════════════════════════════════════════════════
# Base Record
# ══════════════════════════════════════════════════════════════════════════════

class BaseRecord(BaseModel):
    """
    Base class for all dataset records.
    Provides source tracking metadata fields.
    """

    # Source metadata (injected during transformation, not from raw data)
    source_type: Optional[SourceType] = None
    source_file: Optional[UUID] = None
    source_api_endpoint: Optional[str] = None
    source_device_id: Optional[str] = None
    ingestion_method: Optional[str] = "batch"
    ingested_at: Optional[datetime] = None

    model_config = ConfigDict(
        extra="ignore",  # Ignore unexpected fields (like unmapped CSV columns)
        str_strip_whitespace=True,
        validate_default=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# Helper Functions (preserve existing coercion logic)
# ══════════════════════════════════════════════════════════════════════════════

def parse_european_float(v: Any) -> Optional[float]:
    """
    Parse float with European decimal format support (comma → period).
    Preserves logic from transformation.py:coerce_value().
    """
    if v is None or v == '':
        return None

    # Handle NaN/Inf
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        return v

    if isinstance(v, (int, float)):
        return float(v)

    if isinstance(v, str):
        v = v.strip()
        if v == '':
            return None
        # European decimal format: replace comma with period
        v = v.replace(",", ".")
        return float(v)

    return float(v)


def parse_datetime_with_ampm(v: Any) -> datetime:
    """
    Parse datetime with AM/PM format support.
    Preserves logic from transformation.py:coerce_value().
    """
    if isinstance(v, datetime):
        return v

    if hasattr(v, 'to_pydatetime'):  # pandas Timestamp
        return v.to_pydatetime()

    if isinstance(v, str):
        v = v.strip()
        # Handle AM/PM format: "12/25/2024 10:30:00 AM"
        # Use %I for 12-hour clock (not %H which is 24-hour)
        if any(token in v.lower() for token in ('am', 'pm')):
            return datetime.strptime(v, '%m/%d/%Y %I:%M:%S %p')
        # ISO format fallback
        return datetime.fromisoformat(v)

    # Try converting to string and parsing as ISO
    return datetime.fromisoformat(str(v))


def parse_date_with_ampm(v: Any) -> date:
    """
    Parse date with AM/PM format support.
    Preserves logic from transformation.py:coerce_value().
    """
    if isinstance(v, date) and not isinstance(v, datetime):
        return v

    if hasattr(v, 'date'):  # datetime or pandas Timestamp
        return v.date()
    
    if isinstance(v, str):
        v = v.strip()
        # Handle AM/PM format
        if any(token in v.lower() for token in ('am', 'pm')):
            return datetime.strptime(v, '%m/%d/%Y %H:%M:%S %p').date()
        # ISO format fallback
        return datetime.fromisoformat(v).date()

    return datetime.fromisoformat(str(v)).date()


def parse_int_from_float(v: Any) -> Optional[int]:
    """
    Parse integer, handling "123.0" string cases.
    Preserves logic from transformation.py:coerce_value().
    """
    if v is None or v == '':
        return None

    if isinstance(v, int):
        return v

    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        return int(v)

    if isinstance(v, str):
        v = v.strip()
        if v == '':
            return None
        # Handle "123.0" case
        return int(float(v))

    return int(v)


# ══════════════════════════════════════════════════════════════════════════════
# Energy Hourly Record
# ══════════════════════════════════════════════════════════════════════════════

class EnergyHourlyRecord(BaseRecord):
    """
    Pydantic model for fact_energy_hourly table.

    Database schema:
        - device_id: INTEGER FK (injected after validation)
        - ts: TIMESTAMP NOT NULL
        - energy_kwh: FLOAT NOT NULL
        - source_file: UUID FK
    """
    ts: datetime
    energy_kwh: float = Field(ge=0, le=10000, description="Hourly energy consumption in kWh")
    device_id: Optional[int] = None  # FK to device table, injected after validation

    @field_validator('ts', mode='before')
    @classmethod
    def validate_ts(cls, v):
        return parse_datetime_with_ampm(v)

    @field_validator('energy_kwh', mode='before')
    @classmethod
    def validate_energy_kwh(cls, v):
        result = parse_european_float(v)
        return result if result is not None else 0.0


# ══════════════════════════════════════════════════════════════════════════════
# Energy Daily Record
# ══════════════════════════════════════════════════════════════════════════════

class EnergyDailyRecord(BaseRecord):
    """
    Pydantic model for fact_energy_daily table.

    Database schema:
        - device_id: INTEGER FK (injected after validation)
        - ts: DATE NOT NULL
        - energy_kwh: FLOAT NOT NULL
        - source_file: UUID FK
    """
    ts: date
    energy_kwh: float = Field(ge=0, le=100000, description="Daily energy consumption in kWh")
    device_id: Optional[int] = None  # FK to device table, injected after validation
    
    @field_validator('ts', mode='before')
    @classmethod
    def validate_ts(cls, v):
        return parse_date_with_ampm(v)
    
    @field_validator('energy_kwh', mode='before')
    @classmethod
    def validate_energy_kwh(cls, v):
        result = parse_european_float(v)
        return result if result is not None else 0.0


# ══════════════════════════════════════════════════════════════════════════════
# Environmental Metrics Record
# ══════════════════════════════════════════════════════════════════════════════

class EnvironmentalMetricsRecord(BaseRecord):
    """
    Pydantic model for environmental_metrics table.

    Database schema:
        - timestamp: TIMESTAMPTZ NOT NULL (unique)
        - atm_pressure: DECIMAL(8, 2)
        - noise_level_db: DECIMAL(6, 2)
        - temperature: DECIMAL(6, 2)
        - humidity: DECIMAL(6, 2)
        - pm10: DECIMAL(8, 2)
        - wind_speed: DECIMAL(6, 2)
        - wind_direction_sectors: DECIMAL(6, 2)
        - wind_angle: DECIMAL(6, 2)
        - pm2p5: DECIMAL(8, 2)
        - source_file: UUID FK
    """
    timestamp: datetime
    temperature: float = Field(ge=-50, le=60, description="Temperature in Celsius")
    
    # Optional fields with constraints from YAML validation_rules
    atm_pressure: Optional[float] = None
    noise_level_db: Optional[float] = Field(default=None, ge=0, le=140)
    humidity: Optional[float] = Field(default=None, ge=0, le=100)
    pm10: Optional[float] = Field(default=None, ge=0, le=1000)
    wind_speed: Optional[float] = Field(default=None, ge=0, le=150)
    wind_direction_sectors: Optional[float] = None
    wind_angle: Optional[float] = Field(default=None, ge=0, le=360)
    pm2p5: Optional[float] = Field(default=None, ge=0, le=500)
    
    @field_validator('timestamp', mode='before')
    @classmethod
    def validate_timestamp(cls, v):
        return parse_datetime_with_ampm(v)
    
    @field_validator(
        'temperature', 'atm_pressure', 'noise_level_db', 'humidity',
        'pm10', 'wind_speed', 'wind_direction_sectors', 'wind_angle', 'pm2p5',
        mode='before'
    )
    @classmethod
    def validate_floats(cls, v):
        return parse_european_float(v)


# ══════════════════════════════════════════════════════════════════════════════
# Dairy Production Record
# ══════════════════════════════════════════════════════════════════════════════

class DairyProductionRecord(BaseRecord):
    """
    Pydantic model for dairy_production table.
    
    Database schema:
        - production_date: DATE NOT NULL (unique)
        - day_production_per_cow_kg: DECIMAL(10, 2)
        - number_of_animals: INTEGER
        - average_lactation_days: INTEGER
        - fed_per_cow_total_kg: DECIMAL(10, 2)
        - fed_per_cow_water_kg: DECIMAL(10, 2)
        - feed_efficiency: DECIMAL(10, 4)
        - rumination_minutes: INTEGER
        - source_file: UUID FK
    """
    production_date: date
    number_of_animals: int = Field(ge=1, le=10000, description="Number of animals")
    
    # Optional fields with constraints from YAML validation_rules
    day_production_per_cow_kg: Optional[float] = Field(default=None, ge=0, le=100)
    average_lactation_days: Optional[int] = Field(default=None, ge=0, le=1000)
    fed_per_cow_total_kg: Optional[float] = Field(default=None, ge=0, le=200)
    fed_per_cow_water_kg: Optional[float] = Field(default=None, ge=0, le=100)
    feed_efficiency: Optional[float] = Field(default=None, ge=0, le=10)
    rumination_minutes: Optional[int] = Field(default=None, ge=0, le=1440)
    
    @field_validator('production_date', mode='before')
    @classmethod
    def validate_production_date(cls, v):
        return parse_date_with_ampm(v)
    
    @field_validator('number_of_animals', 'average_lactation_days', 'rumination_minutes', mode='before')
    @classmethod
    def validate_ints(cls, v):
        result = parse_int_from_float(v)
        # number_of_animals is required, so return 0 if None (will fail ge=1 constraint)
        return result if result is not None else 0
    
    @field_validator(
        'day_production_per_cow_kg', 'fed_per_cow_total_kg', 
        'fed_per_cow_water_kg', 'feed_efficiency',
        mode='before'
    )
    @classmethod
    def validate_floats(cls, v):
        return parse_european_float(v)


# ══════════════════════════════════════════════════════════════════════════════
# Model Registry
# ══════════════════════════════════════════════════════════════════════════════

MODEL_REGISTRY: Dict[str, type[BaseRecord]] = {
    'energy_hourly': EnergyHourlyRecord,
    'energy_daily': EnergyDailyRecord,
    'environmental_metrics': EnvironmentalMetricsRecord,
    'dairy_production': DairyProductionRecord,
}


def get_model_for_dataset(dataset: str) -> type[BaseRecord]:
    """
    Get Pydantic model class for a dataset name.
    
    Args:
        dataset: Dataset name (e.g., 'energy_hourly', 'environmental_metrics')
    
    Returns:
        Pydantic model class
    
    Raises:
        ValueError: If no model is registered for the dataset
    """
    if dataset not in MODEL_REGISTRY:
        raise ValueError(
            f"No model registered for dataset: {dataset}. "
            f"Available: {list(MODEL_REGISTRY.keys())}"
        )
    return MODEL_REGISTRY[dataset]


def list_datasets() -> list[str]:
    """Return list of all registered dataset names."""
    return list(MODEL_REGISTRY.keys())
