"""
Tests for PydanticTransformer

Verifies column mapping, validation error handling, and source context injection.
"""

import pytest
import yaml
from pathlib import Path

import sys
sys.path.insert(0, '..')

from pydantic_transformer import PydanticTransformer
from validation import ValidationResult


# Fixture: Load real YAML mappings
@pytest.fixture
def energy_hourly_mapping():
    mapping_path = Path(__file__).parent.parent / "mappings" / "energy_hourly.yaml"
    with open(mapping_path) as f:
        return yaml.safe_load(f)


@pytest.fixture
def environmental_mapping():
    mapping_path = Path(__file__).parent.parent / "mappings" / "environmental_metrics.yaml"
    with open(mapping_path) as f:
        return yaml.safe_load(f)


@pytest.fixture
def dairy_mapping():
    mapping_path = Path(__file__).parent.parent / "mappings" / "dairy_production.yaml"
    with open(mapping_path) as f:
        return yaml.safe_load(f)


@pytest.fixture
def source_context():
    return {
        "source_type": "csv",
        "source_file": None,
        "device_id": 123,
        "ingestion_method": "batch"
    }


class TestPydanticTransformerInit:
    """Test transformer initialization."""
    
    def test_init_energy_hourly(self, energy_hourly_mapping):
        """Initialize with energy hourly mapping."""
        transformer = PydanticTransformer(energy_hourly_mapping)
        assert transformer.dataset == "energy_hourly"
        assert "Date and Time" in transformer.columns
    
    def test_init_environmental(self, environmental_mapping):
        """Initialize with environmental mapping."""
        transformer = PydanticTransformer(environmental_mapping)
        assert transformer.dataset == "environmental_metrics"


class TestColumnMapping:
    """Test CSV column → DB column mapping."""
    
    def test_energy_hourly_mapping(self, energy_hourly_mapping, source_context):
        """Map energy hourly CSV columns to DB columns."""
        transformer = PydanticTransformer(energy_hourly_mapping)
        
        # Raw data with CSV column names
        raw_data = {
            "Date and Time": "12/25/2024 10:30:00 AM",
            "Hourly": "123,45"
        }
        
        transformed, result = transformer.transform_and_validate(raw_data, source_context)
        
        assert result.is_valid
        assert "ts" in transformed  # Mapped from "Date and Time"
        assert "energy_kwh" in transformed  # Mapped from "Hourly"
        assert transformed["energy_kwh"] == 123.45
    
    def test_environmental_mapping(self, environmental_mapping, source_context):
        """Map environmental CSV columns to DB columns."""
        transformer = PydanticTransformer(environmental_mapping)
        
        # Raw data with CSV column names (with units)
        raw_data = {
            "timestamp": "2024-12-25T10:30:00",
            "temperature (°C)": "22,5",
            "humidity (%)": "65",
            "pm10 (µg/m³)": "45,5"
        }
        
        transformed, result = transformer.transform_and_validate(raw_data, source_context)
        
        assert result.is_valid
        assert transformed["temperature"] == 22.5
        assert transformed["humidity"] == 65.0
        assert transformed["pm10"] == 45.5
    
    def test_dairy_mapping(self, dairy_mapping, source_context):
        """Map dairy production CSV columns to DB columns."""
        transformer = PydanticTransformer(dairy_mapping)
        
        raw_data = {
            "Date": "2024-12-25",
            "Day production/cow (kg)": "28,5",
            "Nr. animals": "150",
            "Feed efficiency": "1,8"
        }
        
        transformed, result = transformer.transform_and_validate(raw_data, source_context)
        
        assert result.is_valid
        assert transformed["day_production_per_cow_kg"] == 28.5
        assert transformed["number_of_animals"] == 150


class TestValidationErrors:
    """Test validation error handling."""
    
    def test_out_of_range_error(self, environmental_mapping, source_context):
        """Out of range value produces validation error."""
        transformer = PydanticTransformer(environmental_mapping)
        
        raw_data = {
            "timestamp": "2024-12-25T10:30:00",
            "temperature (°C)": "100"  # > 60°C limit
        }
        
        transformed, result = transformer.transform_and_validate(raw_data, source_context)
        
        assert not result.is_valid
        assert transformed is None
        assert len(result.errors) > 0
        assert result.errors[0].field == "temperature"
        assert "60" in result.errors[0].message
    
    def test_type_error(self, energy_hourly_mapping, source_context):
        """Invalid type produces validation error."""
        transformer = PydanticTransformer(energy_hourly_mapping)
        
        raw_data = {
            "Date and Time": "not-a-date",
            "Hourly": "123"
        }
        
        transformed, result = transformer.transform_and_validate(raw_data, source_context)
        
        assert not result.is_valid
        assert transformed is None
        assert any(e.field == "ts" for e in result.errors)
    
    def test_missing_required_field(self, energy_hourly_mapping, source_context):
        """Missing required field produces validation error."""
        transformer = PydanticTransformer(energy_hourly_mapping)
        
        raw_data = {
            "Hourly": "123"
            # Missing "Date and Time"
        }
        
        transformed, result = transformer.transform_and_validate(raw_data, source_context)
        
        assert not result.is_valid
        assert transformed is None


class TestSourceContext:
    """Test source context handling."""
    
    def test_source_file_included(self, energy_hourly_mapping):
        """Source file UUID is included in transformed data."""
        from uuid import UUID
        
        transformer = PydanticTransformer(energy_hourly_mapping)
        
        raw_data = {
            "Date and Time": "2024-12-25T10:30:00",
            "Hourly": "123"
        }
        
        source_context = {
            "source_type": "csv",
            "source_file": UUID("12345678-1234-5678-1234-567812345678"),
            "device_id": 456
        }
        
        transformed, result = transformer.transform_and_validate(raw_data, source_context)
        
        assert result.is_valid
        assert "source_file" in transformed
        assert transformed["source_file"] == source_context["source_file"]
    
    def test_source_file_none(self, energy_hourly_mapping, source_context):
        """No source file when not provided."""
        transformer = PydanticTransformer(energy_hourly_mapping)
        
        raw_data = {
            "Date and Time": "2024-12-25T10:30:00",
            "Hourly": "123"
        }
        
        transformed, result = transformer.transform_and_validate(raw_data, source_context)
        
        assert result.is_valid
        assert "source_file" not in transformed or transformed.get("source_file") is None


class TestBatchTransform:
    """Test batch transformation."""
    
    def test_batch_all_valid(self, energy_hourly_mapping, source_context):
        """Batch transform with all valid records."""
        transformer = PydanticTransformer(energy_hourly_mapping)
        
        raw_records = [
            {"Date and Time": "2024-12-25T10:00:00", "Hourly": "100"},
            {"Date and Time": "2024-12-25T11:00:00", "Hourly": "110"},
            {"Date and Time": "2024-12-25T12:00:00", "Hourly": "120"},
        ]
        
        valid, invalid = transformer.transform_batch(raw_records, source_context)
        
        assert len(valid) == 3
        assert len(invalid) == 0
    
    def test_batch_mixed(self, energy_hourly_mapping, source_context):
        """Batch transform with mixed valid/invalid records."""
        transformer = PydanticTransformer(energy_hourly_mapping)
        
        raw_records = [
            {"Date and Time": "2024-12-25T10:00:00", "Hourly": "100"},
            {"Date and Time": "invalid-date", "Hourly": "110"},  # Invalid
            {"Date and Time": "2024-12-25T12:00:00", "Hourly": "-50"},  # Invalid (negative)
        ]
        
        valid, invalid = transformer.transform_batch(raw_records, source_context)
        
        assert len(valid) == 1
        assert len(invalid) == 2
        
        # Check invalid indices
        invalid_indices = [idx for idx, _ in invalid]
        assert 1 in invalid_indices
        assert 2 in invalid_indices


class TestValidateRecordCompatibility:
    """Test backward-compatible validate_record method."""
    
    def test_validate_record_valid(self, energy_hourly_mapping):
        """validate_record returns valid result."""
        transformer = PydanticTransformer(energy_hourly_mapping)
        
        # Already-mapped data (DB column names)
        data = {
            "ts": "2024-12-25T10:30:00",
            "energy_kwh": 123.45
        }
        
        result = transformer.validate_record(data)
        
        assert result.is_valid
        assert len(result.errors) == 0
    
    def test_validate_record_invalid(self, energy_hourly_mapping):
        """validate_record returns errors for invalid data."""
        transformer = PydanticTransformer(energy_hourly_mapping)
        
        data = {
            "ts": "invalid",
            "energy_kwh": 123.45
        }
        
        result = transformer.validate_record(data)
        
        assert not result.is_valid
        assert len(result.errors) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
