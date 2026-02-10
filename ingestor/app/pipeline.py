"""
Data Pipeline Module
Orchestrates extract → validate → transform → load with full observability.
"""

from typing import Dict, List, Any, Optional
import json
import logging
from datetime import datetime
from uuid import UUID
from dataclasses import dataclass, field

from validation import ValidationResult
from pydantic_transformer import PydanticTransformer
from dao import DAOFactory, StagingDAO, DataDAO, PipelineDAO

logger = logging.getLogger(__name__)


@dataclass
class PipelineMetrics:
    """Metrics for pipeline execution."""
    file_id: Optional[UUID] = None
    pipeline_name: str = "default"

    # Stage metrics
    extract_records: int = 0
    validate_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    transform_records: int = 0
    load_records: int = 0
    skipped_records: int = 0

    # Timing
    started_at: datetime = field(default_factory=datetime.now)
    extract_duration: float = 0.0
    validate_duration: float = 0.0
    transform_duration: float = 0.0
    load_duration: float = 0.0
    total_duration: float = 0.0

    # Errors
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging."""
        return {
            'file_id': str(self.file_id) if self.file_id else None,
            'pipeline_name': self.pipeline_name,
            'records': {
                'extracted': self.extract_records,
                'validated': self.validate_records,
                'valid': self.valid_records,
                'invalid': self.invalid_records,
                'transformed': self.transform_records,
                'loaded': self.load_records,
                'skipped': self.skipped_records
            },
            'duration': {
                'extract': self.extract_duration,
                'validate': self.validate_duration,
                'transform': self.transform_duration,
                'load': self.load_duration,
                'total': self.total_duration
            },
            'errors': self.errors
        }


class DataPipeline:
    """Main pipeline orchestrator."""
    
    def __init__(self, connection, mapping: Dict[str, Any], 
                 source_context: Dict[str, Any]):
        """
        Initialize pipeline.
        
        Args:
            connection: Database connection
            mapping: YAML mapping configuration
            source_context: Source metadata (type, file_id, device_id, etc.)
        """
        self.connection = connection
        self.mapping = mapping
        self.source_context = source_context
        self.dataset = str(mapping.get('dataset'))
        self.device_id = source_context.get('device_id')  # Store device_id
        self.target_table = mapping.get('target_table', self.dataset)
        
        # Initialize DAO factory
        self.dao = DAOFactory(connection)
        
        # Initialize components - using unified PydanticTransformer
        self.pydantic_transformer = PydanticTransformer(mapping)
        self.staging_dao = self.dao.staging(self.dataset)
        
        # Conflict resolution - using DataDAO
        conflict_config = mapping.get('conflict_resolution', {})
        self.data_dao = self.dao.data(conflict_config)
        
        # Pipeline DAO for logging
        self.pipeline_dao = self.dao.pipeline
        
        # Metrics
        self.metrics = PipelineMetrics(
            file_id=source_context.get('source_file'),
            pipeline_name=f"{self.dataset}_pipeline"
        )
    
    def execute(self, raw_records: List[Dict[str, Any]]) -> PipelineMetrics:
        """
        Execute full pipeline: extract → validate → transform → load.
        
        Args:
            raw_records: List of raw data records (CSV rows or API responses)
        
        Returns:
            PipelineMetrics with execution results
        """
        start_time = datetime.now()
        
        try:
            # Stage 1: Extract (already done - just count)
            self._log_stage_start('extract')
            self.metrics.extract_records = len(raw_records)
            self._log_stage_end('extract', self.metrics.extract_records,
                              self.metrics.extract_records)

            # Stage 2: Validate & Transform & Stage
            self._log_stage_start('validate')
            self._validate_and_stage(raw_records)
            self._log_stage_end('validate', self.metrics.extract_records,
                              self.metrics.valid_records)

            # Stage 3: Load valid records
            self._log_stage_start('load')
            self._load_to_final()
            self._log_stage_end('load', self.metrics.valid_records,
                              self.metrics.load_records)
            
            # Calculate total duration
            self.metrics.total_duration = (datetime.now() - start_time).total_seconds()
            
            # Log data quality metrics
            self._log_quality_metrics()
            
            logger.info(f"Pipeline completed: {self.metrics.to_dict()}")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            self.metrics.errors.append(str(e))
            # Rollback failed transaction before logging
            self.connection.rollback()
            self._log_stage_end('load', 0, 0, status='failed', error=str(e))
            raise
        
        return self.metrics
    
    def _validate_and_stage(self, raw_records: List[Dict[str, Any]]):
        """
        Validate, transform, and stage records using PydanticTransformer.
        
        This method uses the unified PydanticTransformer which:
        1. Maps CSV columns to DB columns
        2. Coerces types (with European decimal, AM/PM date support)
        3. Validates constraints (min/max, required fields)
        4. Returns transformed data or validation errors
        """
        stage_start = datetime.now()
        
        for idx, raw_record in enumerate(raw_records):
            row_number = idx + 1

            # Insert to staging (raw) using DAO
            staging_id = self.staging_dao.insert_raw(
                file_id=self.source_context.get('source_file'), # type: ignore
                row_number=row_number,
                raw_data=raw_record
            )

            # Transform + Validate in one step using Pydantic
            try:
                transformed, validation_result = self.pydantic_transformer.transform_and_validate(
                    raw_record,
                    self.source_context
                )

                if validation_result.is_valid and transformed is not None:
                    # Update staging with transformed data using DAO
                    self.staging_dao.update_validation(
                        staging_id=staging_id,
                        is_valid=True,
                        validation_errors=None,
                        transformed_data=transformed
                    )

                    self.metrics.valid_records += 1
                    self.metrics.transform_records += 1

                else:
                    # Record validation errors using DAO
                    self.staging_dao.update_validation(
                        staging_id=staging_id,
                        is_valid=False,
                        validation_errors=validation_result.to_dict() if validation_result else None
                    )
                    self.metrics.invalid_records += 1

                    # Log first few validation errors for debugging
                    if self.metrics.invalid_records <= 5:
                        logger.warning(
                            f"Validation failed for row {row_number}: "
                            f"{[str(e) for e in validation_result.errors[:3]]}"
                        )

            except Exception as e:
                logger.warning(f"Transform/validate failed for row {row_number}: {e}")
                # Mark as invalid using DAO
                self.staging_dao.update_validation(
                    staging_id=staging_id,
                    is_valid=False,
                    validation_errors={'errors': [{'error_type': 'transformation', 'message': str(e)}]}
                )
                self.metrics.invalid_records += 1

            self.metrics.validate_records += 1
        
        self.metrics.validate_duration = (datetime.now() - stage_start).total_seconds()
        
        # Commit staging changes
        self.dao.commit()
    
    def _load_to_final(self):
        """Load valid records from staging to final table."""
        load_start = datetime.now()
        
        # Get valid records from staging using DAO
        valid_records = self.staging_dao.get_valid_records(
            file_id=self.source_context.get('source_file')
        )
        
        if not valid_records:
            logger.info("No valid records to load")
            self.metrics.load_duration = (datetime.now() - load_start).total_seconds()
            return
        
        loaded_staging_ids = []

        for record in valid_records:
            staging_id = record.pop('_staging_id')
            
            try:
                # Add device_id if applicable
                if 'energy' in str(self.target_table) and 'device_id' not in record:
                    record['device_id'] = self.device_id
                
                # Insert with conflict resolution using DataDAO
                success = self.data_dao.insert_record(self.target_table, record)
                
                if success:
                    self.metrics.load_records += 1
                    loaded_staging_ids.append(staging_id)
                else:
                    self.metrics.skipped_records += 1
                    
            except Exception as e:
                logger.error(f"Failed to load record from staging {staging_id}: {e}")
                self.metrics.errors.append(f"Load error: {e}")
        
        # Mark loaded records using DAO
        if loaded_staging_ids:
            self.staging_dao.mark_loaded(loaded_staging_ids)
        
        # Commit final loads
        self.dao.commit()
        
        self.metrics.load_duration = (datetime.now() - load_start).total_seconds()
    
    def _log_stage_start(self, stage: str):
        """Log pipeline stage start using DAO."""
        self.pipeline_dao.log_stage_start(
            file_id=self.source_context.get('source_file'),
            pipeline_name=self.metrics.pipeline_name,
            stage=stage,
            source_type=self.source_context.get('source_type'),
            dataset=self.dataset
        )
    
    def _log_stage_end(self, stage: str, records_in: int, records_out: int,
                       status: str = 'success', error: Optional[str] = None):
        """Log pipeline stage completion using DAO."""
        self.pipeline_dao.log_stage_end(
            pipeline_name=self.metrics.pipeline_name,
            stage=stage,
            records_in=records_in,
            records_out=records_out,
            status=status,
            error_message=error
        )
    
    def _log_quality_metrics(self):
        """Log data quality check results using DAO."""
        # Get validation error samples
        invalid_records = self.staging_dao.get_invalid_records(
            file_id=self.source_context.get('source_file')
        )
        
        # Group errors by type
        error_types = {}
        for record in invalid_records[:100]:  # Sample first 100
            errors = record.get('validation_errors', {}).get('errors', [])
            for error in errors:
                error_type = error.get('error_type', 'unknown')
                if error_type not in error_types:
                    error_types[error_type] = []
                error_types[error_type].append({
                    'row': record['row_number'],
                    'field': error.get('field'),
                    'message': error.get('message')
                })
        
        # Log each quality check type using DAO
        self.pipeline_dao.log_quality_checks_batch(
            file_id=self.source_context.get('source_file'),
            dataset=self.dataset,
            error_types=error_types,
            total_count=self.metrics.extract_records
        )
        
        # Overall quality check
        self.pipeline_dao.log_quality_check(
            file_id=self.source_context.get('source_file'),
            dataset=self.dataset,
            check_type='overall',
            check_name='overall_validation',
            passed=self.metrics.invalid_records == 0,
            failed_count=self.metrics.invalid_records,
            total_count=self.metrics.extract_records,
            failure_rate=round((self.metrics.invalid_records / self.metrics.extract_records * 100) 
                  if self.metrics.extract_records > 0 else 0, 2),
            sample_failures={'summary': f'{self.metrics.valid_records} valid, {self.metrics.invalid_records} invalid'}
        )
        
        self.dao.commit()

# Deprecated
def run_csv_pipeline(file_path: str, connection, mapping: Dict[str, Any],
                     source_file_id: Optional[UUID] = None,
                     device_id: Optional[int] = None) -> PipelineMetrics:
    """
    Convenience function to run pipeline for CSV file.

    Args:
        file_path: Path to CSV file
        connection: Database connection
        mapping: YAML mapping configuration
        source_file_id: Optional file ID for tracking
        device_id: Optional device identifier
    
    Returns:
        PipelineMetrics
    """
    # Build source context
    file_ext = os.path.splitext(file_path)[1].lower()
    source_context = {
        'source_type': 'csv' if file_ext == '.csv' else 'excel',
        'source_file': source_file_id,
        'device_id': device_id,
        'ingestion_method': 'batch'
    }
    
    # Extract data (CSV or Excel)
    raw_records = []
    if file_ext in ['.xlsx', '.xls']:
        # Handle Excel files
        try:
            import pandas as pd
            import numpy as np
            df = pd.read_excel(file_path)
            
            # Clean the dataframe
            # 1. Remove summary rows (AVG, SUM, TOTAL, etc.)
            if 'Date' in df.columns:
                df = df[~df['Date'].astype(str).str.upper().isin(['AVG', 'SUM', 'TOTAL', 'AVERAGE'])]
            
            # 2. Remove rows where all values are NaN
            df = df.dropna(how='all')

            # 3. Replace NaN with None for JSON compatibility
            df = df.replace({np.nan: None, pd.NA: None, "": None})
            df = df.where(pd.notnull(df), None) # type: ignore

            # 4. Convert all datetime objects in the DataFrame to ISO strings for JSON serialization
            for col in df.columns:
                df[col] = df[col].apply(
                    lambda x: x.isoformat() if isinstance(x, (datetime, pd.Timestamp)) else x
                )
            
            # 5. Convert to records
            raw_records = df.to_dict('records')

            logger.info(f"Extracted {len(raw_records)} records from Excel file (after cleaning)")
        except ImportError:
            raise ImportError("pandas and openpyxl are required to read Excel files. Install with: pip install pandas openpyxl")
    else:
        # Handle CSV files
        with open(file_path, 'r', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            raw_records = list(reader)

    # Run pipeline
    pipeline = DataPipeline(connection, mapping, source_context)
    return pipeline.execute(raw_records)

# Deprecated
def run_api_pipeline(api_records: List[Dict[str, Any]], connection,
                     mapping: Dict[str, Any], api_endpoint: str,
                     device_id: Optional[str] = None) -> PipelineMetrics:
    """
    Convenience function to run pipeline for API data.
    
    Args:
        api_records: List of records from API response
        connection: Database connection
        mapping: YAML mapping configuration
        api_endpoint: API endpoint URL
        device_id: Optional device identifier
    
    Returns:
        PipelineMetrics
    """
    # Build source context
    source_context = {
        'source_type': 'api',
        'api_endpoint': api_endpoint,
        'device_id': device_id,
        'ingestion_method': 'streaming'
    }
    
    # Run pipeline
    pipeline = DataPipeline(connection, mapping, source_context)
    return pipeline.execute(api_records)
