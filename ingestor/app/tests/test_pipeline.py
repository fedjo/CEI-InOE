"""
Tests for Data Pipeline Integration

Tests the full pipeline flow: file reading → transform → validate → stage → load.
Uses mocked database connections for unit tests.
"""

import pytest
import csv
import os
import tempfile
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime
from uuid import UUID, uuid4
import yaml
from pathlib import Path

import sys
sys.path.insert(0, '..')

from pipeline import DataPipeline, PipelineMetrics, run_csv_pipeline


# ═══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def energy_hourly_mapping():
    """Load energy hourly YAML mapping."""
    mapping_path = Path(__file__).parent.parent / "mappings" / "energy_hourly.yaml"
    with open(mapping_path) as f:
        return yaml.safe_load(f)


@pytest.fixture
def environmental_mapping():
    """Load environmental metrics YAML mapping."""
    mapping_path = Path(__file__).parent.parent / "mappings" / "environmental_metrics.yaml"
    with open(mapping_path) as f:
        return yaml.safe_load(f)


@pytest.fixture
def mock_connection():
    """Create a mock database connection."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    cursor.fetchone.return_value = (1,)  # Return staging_id
    return conn


@pytest.fixture
def source_context():
    """Standard source context for tests."""
    return {
        "source_type": "csv",
        "source_file": uuid4(),
        "device_id": 1,  # Integer device_id FK
        "ingestion_method": "batch"
    }


@pytest.fixture
def temp_csv_energy():
    """Create a temporary CSV file with energy data."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        writer = csv.writer(f)
        writer.writerow(["Date and Time", "Hourly"])
        writer.writerow(["12/25/2024 10:00:00 AM", "100.5"])
        writer.writerow(["12/25/2024 11:00:00 AM", "110.3"])
        writer.writerow(["12/25/2024 12:00:00 PM", "95.8"])
        f.flush()
        yield f.name
    os.unlink(f.name)


@pytest.fixture
def temp_csv_environmental():
    """Create a temporary CSV file with environmental data."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "temperature (°C)", "humidity (%)", "pm10 (µg/m³)"])
        writer.writerow(["2024-12-25T10:00:00", "22.5", "65", "45.5"])
        writer.writerow(["2024-12-25T11:00:00", "23.0", "63", "42.0"])
        writer.writerow(["2024-12-25T12:00:00", "24.5", "60", "38.5"])
        f.flush()
        yield f.name
    os.unlink(f.name)


@pytest.fixture
def temp_csv_with_errors():
    """Create a CSV file with some invalid records."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "temperature (°C)", "humidity (%)"])
        writer.writerow(["2024-12-25T10:00:00", "22.5", "65"])  # Valid
        writer.writerow(["2024-12-25T11:00:00", "100", "63"])   # Invalid: temp > 60
        writer.writerow(["invalid-timestamp", "20", "60"])      # Invalid: bad timestamp
        writer.writerow(["2024-12-25T13:00:00", "25", "150"])   # Invalid: humidity > 100
        f.flush()
        yield f.name
    os.unlink(f.name)


# ═══════════════════════════════════════════════════════════════════════════════
# Pipeline Metrics Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestPipelineMetrics:
    """Test PipelineMetrics dataclass."""
    
    def test_default_values(self):
        """Default metric values."""
        metrics = PipelineMetrics()
        assert metrics.extract_records == 0
        assert metrics.valid_records == 0
        assert metrics.invalid_records == 0
        assert metrics.load_records == 0
        assert len(metrics.errors) == 0
    
    def test_to_dict(self):
        """Convert metrics to dictionary."""
        metrics = PipelineMetrics(
            pipeline_name="test_pipeline",
            extract_records=100,
            valid_records=95,
            invalid_records=5,
            load_records=95
        )
        
        d = metrics.to_dict()
        
        assert d["pipeline_name"] == "test_pipeline"
        assert d["records"]["extracted"] == 100
        assert d["records"]["valid"] == 95
        assert d["records"]["invalid"] == 5


# ═══════════════════════════════════════════════════════════════════════════════
# DataPipeline Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestDataPipelineInit:
    """Test DataPipeline initialization."""
    
    def test_init_creates_transformer(self, mock_connection, energy_hourly_mapping, source_context):
        """Pipeline creates PydanticTransformer."""
        pipeline = DataPipeline(mock_connection, energy_hourly_mapping, source_context)
        
        assert pipeline.pydantic_transformer is not None
        assert pipeline.dataset == "energy_hourly"
        assert pipeline.target_table == "fact_energy_hourly"
    
    def test_init_creates_staging_manager(self, mock_connection, energy_hourly_mapping, source_context):
        """Pipeline creates StagingDAO."""
        pipeline = DataPipeline(mock_connection, energy_hourly_mapping, source_context)
        
        assert pipeline.staging_dao is not None
    
    def test_init_creates_conflict_resolver(self, mock_connection, energy_hourly_mapping, source_context):
        """Pipeline creates DataDAO."""
        pipeline = DataPipeline(mock_connection, energy_hourly_mapping, source_context)
        
        assert pipeline.data_dao is not None


class TestDataPipelineExecute:
    """Test DataPipeline.execute() method."""
    
    def test_execute_counts_records(self, mock_connection, energy_hourly_mapping, source_context):
        """Execute counts extracted records."""
        # Mock DAOFactory to not require real DB
        with patch('pipeline.DAOFactory') as MockDAOFactory:
            mock_dao = MockDAOFactory.return_value
            mock_staging = MagicMock()
            mock_staging.insert_raw.return_value = 1
            mock_staging.get_valid_records.return_value = []
            mock_staging.get_invalid_records.return_value = []
            mock_dao.staging.return_value = mock_staging
            mock_dao.data.return_value = MagicMock()
            mock_dao.pipeline = MagicMock()
            
            pipeline = DataPipeline(mock_connection, energy_hourly_mapping, source_context)
            
            raw_records = [
                {"Date and Time": "2024-12-25T10:00:00", "Hourly": "100"},
                {"Date and Time": "2024-12-25T11:00:00", "Hourly": "110"},
            ]
            
            metrics = pipeline.execute(raw_records)
            
            assert metrics.extract_records == 2
    
    def test_execute_tracks_valid_invalid(self, mock_connection, environmental_mapping, source_context):
        """Execute tracks valid and invalid records."""
        with patch('pipeline.DAOFactory') as MockDAOFactory:
            mock_dao = MockDAOFactory.return_value
            mock_staging = MagicMock()
            mock_staging.insert_raw.return_value = 1
            mock_staging.get_valid_records.return_value = []
            mock_staging.get_invalid_records.return_value = []
            mock_dao.staging.return_value = mock_staging
            mock_dao.data.return_value = MagicMock()
            mock_dao.pipeline = MagicMock()
            
            pipeline = DataPipeline(mock_connection, environmental_mapping, source_context)
            
            raw_records = [
                {"timestamp": "2024-12-25T10:00:00", "temperature (°C)": "22.5"},  # Valid
                {"timestamp": "2024-12-25T11:00:00", "temperature (°C)": "100"},   # Invalid
            ]
            
            metrics = pipeline.execute(raw_records)
            
            assert metrics.valid_records == 1
            assert metrics.invalid_records == 1


# ═══════════════════════════════════════════════════════════════════════════════
# CSV Pipeline Integration Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestRunCsvPipeline:
    """Test run_csv_pipeline convenience function."""
    
    def test_reads_csv_file(self, mock_connection, energy_hourly_mapping, temp_csv_energy):
        """Pipeline reads CSV file correctly."""
        with patch('pipeline.DAOFactory') as MockDAOFactory:
            mock_dao = MockDAOFactory.return_value
            mock_staging = MagicMock()
            mock_staging.insert_raw.return_value = 1
            mock_staging.get_valid_records.return_value = []
            mock_staging.get_invalid_records.return_value = []
            mock_dao.staging.return_value = mock_staging
            mock_dao.data.return_value = MagicMock()
            mock_dao.pipeline = MagicMock()
            
            metrics = run_csv_pipeline(
                file_path=temp_csv_energy,
                connection=mock_connection,
                mapping=energy_hourly_mapping,
                source_file_id=uuid4(),
                device_id=1
            )
            
            assert metrics.extract_records == 3
    
    def test_processes_environmental_data(self, mock_connection, environmental_mapping, temp_csv_environmental):
        """Pipeline processes environmental CSV correctly."""
        with patch('pipeline.DAOFactory') as MockDAOFactory:
            mock_dao = MockDAOFactory.return_value
            mock_staging = MagicMock()
            mock_staging.insert_raw.return_value = 1
            mock_staging.get_valid_records.return_value = []
            mock_staging.get_invalid_records.return_value = []
            mock_dao.staging.return_value = mock_staging
            mock_dao.data.return_value = MagicMock()
            mock_dao.pipeline = MagicMock()
            
            metrics = run_csv_pipeline(
                file_path=temp_csv_environmental,
                connection=mock_connection,
                mapping=environmental_mapping,
                source_file_id=uuid4()
            )
            
            assert metrics.extract_records == 3
            assert metrics.valid_records == 3  # All should be valid
    
    def test_handles_validation_errors(self, mock_connection, environmental_mapping, temp_csv_with_errors):
        """Pipeline handles validation errors correctly."""
        with patch('pipeline.DAOFactory') as MockDAOFactory:
            mock_dao = MockDAOFactory.return_value
            mock_staging = MagicMock()
            mock_staging.insert_raw.return_value = 1
            mock_staging.get_valid_records.return_value = []
            mock_staging.get_invalid_records.return_value = []
            mock_dao.staging.return_value = mock_staging
            mock_dao.data.return_value = MagicMock()
            mock_dao.pipeline = MagicMock()
            
            metrics = run_csv_pipeline(
                file_path=temp_csv_with_errors,
                connection=mock_connection,
                mapping=environmental_mapping,
                source_file_id=uuid4()
            )
            
            assert metrics.extract_records == 4
            assert metrics.valid_records == 1  # Only first record is valid
            assert metrics.invalid_records == 3


# ═══════════════════════════════════════════════════════════════════════════════
# End-to-End Validation Flow Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestValidationFlow:
    """Test the validation flow through the pipeline."""
    
    def test_european_decimal_handling(self, mock_connection, energy_hourly_mapping, source_context):
        """European decimal format (comma) is handled correctly."""
        with patch('pipeline.DAOFactory') as MockDAOFactory:
            mock_dao = MockDAOFactory.return_value
            mock_staging = MagicMock()
            mock_staging.insert_raw.return_value = 1
            mock_staging.get_valid_records.return_value = []
            mock_staging.get_invalid_records.return_value = []
            mock_dao.staging.return_value = mock_staging
            mock_dao.data.return_value = MagicMock()
            mock_dao.pipeline = MagicMock()
            
            pipeline = DataPipeline(mock_connection, energy_hourly_mapping, source_context)
            
            raw_records = [
                {"Date and Time": "2024-12-25T10:00:00", "Hourly": "123,45"},  # European format
            ]
            
            metrics = pipeline.execute(raw_records)
            
            assert metrics.valid_records == 1
            
            # Verify the transformed data was passed to staging via update_validation
            call_args = mock_staging.update_validation.call_args
            # update_validation signature: staging_id, is_valid, validation_errors, transformed_data
            transformed_data = call_args.kwargs.get('transformed_data')
            assert transformed_data["energy_kwh"] == 123.45
    
    def test_ampm_timestamp_handling(self, mock_connection, energy_hourly_mapping, source_context):
        """AM/PM timestamp format is handled correctly."""
        with patch('pipeline.DAOFactory') as MockDAOFactory:
            mock_dao = MockDAOFactory.return_value
            mock_staging = MagicMock()
            mock_staging.insert_raw.return_value = 1
            mock_staging.get_valid_records.return_value = []
            mock_staging.get_invalid_records.return_value = []
            mock_dao.staging.return_value = mock_staging
            mock_dao.data.return_value = MagicMock()
            mock_dao.pipeline = MagicMock()
            
            pipeline = DataPipeline(mock_connection, energy_hourly_mapping, source_context)
            
            raw_records = [
                {"Date and Time": "12/25/2024 10:30:00 AM", "Hourly": "100"},
                {"Date and Time": "12/25/2024 10:30:00 PM", "Hourly": "50"},
            ]
            
            metrics = pipeline.execute(raw_records)
            
            assert metrics.valid_records == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
