"""
Tests for Pydantic Models

Verifies type coercion, validation constraints, and edge cases.
"""

import pytest
from datetime import datetime, date
from pydantic import ValidationError

import sys
sys.path.insert(0, '..')

from models import (
    EnergyHourlyRecord,
    EnergyDailyRecord,
    EnvironmentalMetricsRecord,
    DairyProductionRecord,
    get_model_for_dataset,
    list_datasets,
    parse_european_float,
    parse_datetime_with_ampm,
    parse_date_with_ampm,
)


class TestHelperFunctions:
    """Test helper coercion functions."""
    
    def test_parse_european_float_comma(self):
        """European decimal format: comma → period."""
        assert parse_european_float("123,45") == 123.45
        # Note: thousands separators (1.234,56) are not supported
    
    def test_parse_european_float_period(self):
        """Standard decimal format."""
        assert parse_european_float("123.45") == 123.45
    
    def test_parse_european_float_none(self):
        """None and empty values."""
        assert parse_european_float(None) is None
        assert parse_european_float("") is None
        assert parse_european_float("  ") is None
    
    def test_parse_european_float_nan(self):
        """NaN values return None."""
        import math
        assert parse_european_float(float('nan')) is None
        assert parse_european_float(float('inf')) is None
    
    def test_parse_datetime_ampm(self):
        """AM/PM datetime format."""
        result = parse_datetime_with_ampm("12/25/2024 10:30:00 AM")
        assert result == datetime(2024, 12, 25, 10, 30, 0)
    
    def test_parse_datetime_ampm_pm(self):
        """PM datetime format."""
        result = parse_datetime_with_ampm("12/25/2024 10:30:00 PM")
        assert result == datetime(2024, 12, 25, 22, 30, 0)
    
    def test_parse_datetime_iso(self):
        """ISO datetime format."""
        result = parse_datetime_with_ampm("2024-12-25T10:30:00")
        assert result == datetime(2024, 12, 25, 10, 30, 0)
    
    def test_parse_date_ampm(self):
        """AM/PM date format (extracts date only)."""
        result = parse_date_with_ampm("12/25/2024 10:30:00 AM")
        assert result == date(2024, 12, 25)
    
    def test_parse_date_iso(self):
        """ISO date format."""
        result = parse_date_with_ampm("2024-12-25")
        assert result == date(2024, 12, 25)


class TestEnergyHourlyRecord:
    """Test EnergyHourlyRecord model."""
    
    def test_valid_record(self):
        """Valid energy hourly record."""
        data = {"ts": "2024-12-25T10:30:00", "energy_kwh": 123.45}
        record = EnergyHourlyRecord.model_validate(data)
        assert record.ts == datetime(2024, 12, 25, 10, 30, 0)
        assert record.energy_kwh == 123.45
    
    def test_european_decimal(self):
        """European decimal format (comma)."""
        data = {"ts": "2024-12-25T10:30:00", "energy_kwh": "123,45"}
        record = EnergyHourlyRecord.model_validate(data)
        assert record.energy_kwh == 123.45
    
    def test_ampm_timestamp(self):
        """AM/PM timestamp format."""
        data = {"ts": "12/25/2024 10:30:00 AM", "energy_kwh": 100}
        record = EnergyHourlyRecord.model_validate(data)
        assert record.ts == datetime(2024, 12, 25, 10, 30, 0)
    
    def test_constraint_min(self):
        """Energy cannot be negative."""
        data = {"ts": "2024-12-25T10:30:00", "energy_kwh": -10}
        with pytest.raises(ValidationError) as exc_info:
            EnergyHourlyRecord.model_validate(data)
        assert "greater than or equal to 0" in str(exc_info.value)
    
    def test_constraint_max(self):
        """Energy cannot exceed 10000 kWh."""
        data = {"ts": "2024-12-25T10:30:00", "energy_kwh": 15000}
        with pytest.raises(ValidationError) as exc_info:
            EnergyHourlyRecord.model_validate(data)
        assert "less than or equal to 10000" in str(exc_info.value)
    
    def test_missing_required_field(self):
        """Missing required timestamp."""
        data = {"energy_kwh": 100}
        with pytest.raises(ValidationError) as exc_info:
            EnergyHourlyRecord.model_validate(data)
        assert "ts" in str(exc_info.value)


class TestEnergyDailyRecord:
    """Test EnergyDailyRecord model."""
    
    def test_valid_record(self):
        """Valid energy daily record."""
        data = {"ts": "2024-12-25", "energy_kwh": 1000}
        record = EnergyDailyRecord.model_validate(data)
        assert record.ts == date(2024, 12, 25)
        assert record.energy_kwh == 1000
    
    def test_datetime_to_date(self):
        """Datetime is converted to date."""
        data = {"ts": "2024-12-25T10:30:00", "energy_kwh": 1000}
        record = EnergyDailyRecord.model_validate(data)
        assert record.ts == date(2024, 12, 25)
    
    def test_constraint_max(self):
        """Daily energy cannot exceed 100000 kWh."""
        data = {"ts": "2024-12-25", "energy_kwh": 150000}
        with pytest.raises(ValidationError):
            EnergyDailyRecord.model_validate(data)


class TestEnvironmentalMetricsRecord:
    """Test EnvironmentalMetricsRecord model."""
    
    def test_valid_record(self):
        """Valid environmental metrics record."""
        data = {
            "timestamp": "2024-12-25T10:30:00",
            "temperature": 22.5,
            "humidity": 65,
            "pm10": 45.5
        }
        record = EnvironmentalMetricsRecord.model_validate(data)
        assert record.temperature == 22.5
        assert record.humidity == 65
        assert record.pm10 == 45.5
    
    def test_european_decimals(self):
        """European decimal format for all float fields."""
        data = {
            "timestamp": "2024-12-25T10:30:00",
            "temperature": "22,5",
            "humidity": "65,0",
            "pm10": "45,5"
        }
        record = EnvironmentalMetricsRecord.model_validate(data)
        assert record.temperature == 22.5
        assert record.humidity == 65.0
        assert record.pm10 == 45.5
    
    def test_temperature_constraint_min(self):
        """Temperature cannot be below -50°C."""
        data = {"timestamp": "2024-12-25T10:30:00", "temperature": -60}
        with pytest.raises(ValidationError):
            EnvironmentalMetricsRecord.model_validate(data)
    
    def test_temperature_constraint_max(self):
        """Temperature cannot exceed 60°C."""
        data = {"timestamp": "2024-12-25T10:30:00", "temperature": 70}
        with pytest.raises(ValidationError):
            EnvironmentalMetricsRecord.model_validate(data)
    
    def test_humidity_constraint(self):
        """Humidity must be 0-100%."""
        data = {"timestamp": "2024-12-25T10:30:00", "temperature": 20, "humidity": 150}
        with pytest.raises(ValidationError):
            EnvironmentalMetricsRecord.model_validate(data)
    
    def test_optional_fields(self):
        """Optional fields can be None."""
        data = {"timestamp": "2024-12-25T10:30:00", "temperature": 20}
        record = EnvironmentalMetricsRecord.model_validate(data)
        assert record.humidity is None
        assert record.pm10 is None
        assert record.wind_speed is None


class TestDairyProductionRecord:
    """Test DairyProductionRecord model."""
    
    def test_valid_record(self):
        """Valid dairy production record."""
        data = {
            "production_date": "2024-12-25",
            "number_of_animals": 150,
            "day_production_per_cow_kg": 28.5,
            "feed_efficiency": 1.8
        }
        record = DairyProductionRecord.model_validate(data)
        assert record.production_date == date(2024, 12, 25)
        assert record.number_of_animals == 150
        assert record.day_production_per_cow_kg == 28.5
    
    def test_european_decimals(self):
        """European decimal format."""
        data = {
            "production_date": "2024-12-25",
            "number_of_animals": "150",
            "day_production_per_cow_kg": "28,5",
            "feed_efficiency": "1,8"
        }
        record = DairyProductionRecord.model_validate(data)
        assert record.day_production_per_cow_kg == 28.5
        assert record.feed_efficiency == 1.8
    
    def test_int_from_float_string(self):
        """Integer fields parsed from float strings."""
        data = {
            "production_date": "2024-12-25",
            "number_of_animals": "150.0",
            "rumination_minutes": "480.0"
        }
        record = DairyProductionRecord.model_validate(data)
        assert record.number_of_animals == 150
        assert record.rumination_minutes == 480
    
    def test_animals_constraint_min(self):
        """Must have at least 1 animal."""
        data = {"production_date": "2024-12-25", "number_of_animals": 0}
        with pytest.raises(ValidationError):
            DairyProductionRecord.model_validate(data)
    
    def test_production_constraint_max(self):
        """Production per cow cannot exceed 100 kg."""
        data = {
            "production_date": "2024-12-25",
            "number_of_animals": 100,
            "day_production_per_cow_kg": 150
        }
        with pytest.raises(ValidationError):
            DairyProductionRecord.model_validate(data)


class TestModelRegistry:
    """Test model registry functions."""
    
    def test_list_datasets(self):
        """All datasets are registered."""
        datasets = list_datasets()
        assert "energy_hourly" in datasets
        assert "energy_daily" in datasets
        assert "environmental_metrics" in datasets
        assert "dairy_production" in datasets
    
    def test_get_model_for_dataset(self):
        """Correct model returned for each dataset."""
        assert get_model_for_dataset("energy_hourly") == EnergyHourlyRecord
        assert get_model_for_dataset("energy_daily") == EnergyDailyRecord
        assert get_model_for_dataset("environmental_metrics") == EnvironmentalMetricsRecord
        assert get_model_for_dataset("dairy_production") == DairyProductionRecord
    
    def test_get_model_unknown_dataset(self):
        """Unknown dataset raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            get_model_for_dataset("unknown_dataset")
        assert "No model registered" in str(exc_info.value)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
