"""
Data Pipeline Module
Orchestrates extract → validate → transform → load with full observability.
"""

from typing import Dict, List, Any, Optional
import csv
import json
import logging
from datetime import datetime
from uuid import UUID, uuid4
from dataclasses import dataclass, field

from .validation import DataValidator, ValidationResult
from .transformation import DataTransformer
from .staging import StagingManager, ConflictResolver

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
        self.dataset = mapping.get('dataset')
        self.target_table = mapping.get('target_table', self.dataset)
        
        # Initialize components
        self.validator = DataValidator(mapping)
        self.transformer = DataTransformer(mapping)
        self.staging = StagingManager(connection, self.dataset)
        
        # Conflict resolution
        conflict_config = mapping.get('conflict_resolution', {})
        self.conflict_resolver = ConflictResolver(conflict_config)
        
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
            self._log_stage_end('load', 0, 0, status='failed', error=str(e))
            raise
        
        return self.metrics
    
    def _validate_and_stage(self, raw_records: List[Dict[str, Any]]):
        """Validate, transform, and stage records."""
        stage_start = datetime.now()
        
        for idx, raw_record in enumerate(raw_records):
            row_number = idx + 1
            
            # Insert to staging (raw)
            staging_id = self.staging.insert_raw(
                file_id=self.source_context.get('source_file'),
                row_number=row_number,
                raw_data=raw_record
            )
            
            # Validate
            validation_result = self.validator.validate_record(raw_record)
            
            if validation_result.is_valid:
                # Transform
                try:
                    transformed = self.transformer.transform_record(
                        raw_record, 
                        self.source_context
                    )
                    
                    # Update staging with transformed data
                    self.staging.update_validation(
                        staging_id, 
                        validation_result, 
                        transformed
                    )
                    
                    self.metrics.valid_records += 1
                    self.metrics.transform_records += 1
                    
                except Exception as e:
                    logger.warning(f"Transformation failed for row {row_number}: {e}")
                    # Mark as invalid
                    validation_result.add_error(
                        'transformation', raw_record, 'transform_error', str(e)
                    )
                    self.staging.update_validation(staging_id, validation_result)
                    self.metrics.invalid_records += 1
            else:
                # Record validation errors
                self.staging.update_validation(staging_id, validation_result)
                self.metrics.invalid_records += 1
                
                # Log first few validation errors for debugging
                if self.metrics.invalid_records <= 5:
                    logger.warning(
                        f"Validation failed for row {row_number}: "
                        f"{validation_result.errors[:3]}"
                    )
            
            self.metrics.validate_records += 1
        
        self.metrics.validate_duration = (datetime.now() - stage_start).total_seconds()
        
        # Commit staging changes
        self.connection.commit()
    
    def _load_to_final(self):
        """Load valid records from staging to final table."""
        load_start = datetime.now()
        
        # Get valid records from staging
        valid_records = self.staging.get_valid_records(
            file_id=self.source_context.get('source_file')
        )
        
        if not valid_records:
            logger.info("No valid records to load")
            self.metrics.load_duration = (datetime.now() - load_start).total_seconds()
            return
        
        cursor = self.connection.cursor()
        loaded_staging_ids = []
        
        for record in valid_records:
            staging_id = record.pop('_staging_id')
            
            try:
                # Insert with conflict resolution
                success = self.conflict_resolver.execute_insert(
                    cursor, 
                    self.target_table, 
                    record
                )
                
                if success:
                    self.metrics.load_records += 1
                    loaded_staging_ids.append(staging_id)
                else:
                    self.metrics.skipped_records += 1
                    
            except Exception as e:
                logger.error(f"Failed to load record from staging {staging_id}: {e}")
                self.metrics.errors.append(f"Load error: {e}")
        
        # Mark loaded records
        if loaded_staging_ids:
            self.staging.mark_loaded(loaded_staging_ids)
        
        # Commit final loads
        self.connection.commit()
        
        self.metrics.load_duration = (datetime.now() - load_start).total_seconds()
    
    def _log_stage_start(self, stage: str):
        """Log pipeline stage start."""
        cursor = self.connection.cursor()
        
        sql = """
            INSERT INTO pipeline_execution 
                (file_id, pipeline_name, stage, started_at, status, execution_metadata)
            VALUES (%s, %s, %s, NOW(), 'running', %s)
        """
        
        metadata = {
            'dataset': self.dataset,
            'source_type': self.source_context.get('source_type')
        }
        
        cursor.execute(sql, (
            str(self.source_context.get('source_file')) if self.source_context.get('source_file') else None,
            self.metrics.pipeline_name,
            stage,
            json.dumps(metadata)
        ))
        
        self.connection.commit()
    
    def _log_stage_end(self, stage: str, records_in: int, records_out: int,
                       status: str = 'success', error: Optional[str] = None):
        """Log pipeline stage completion."""
        cursor = self.connection.cursor()
        
        sql = """
            UPDATE pipeline_execution
            SET 
                completed_at = NOW(),
                status = %s,
                records_in = %s,
                records_out = %s,
                error_message = %s
            WHERE pipeline_name = %s 
              AND stage = %s 
              AND status = 'running'
              AND started_at >= NOW() - INTERVAL '1 hour'
            ORDER BY started_at DESC
            LIMIT 1
        """
        
        cursor.execute(sql, (
            status,
            records_in,
            records_out,
            error,
            self.metrics.pipeline_name,
            stage
        ))
        
        self.connection.commit()
    
    def _log_quality_metrics(self):
        """Log data quality check results."""
        cursor = self.connection.cursor()
        
        # Get validation error samples
        invalid_records = self.staging.get_invalid_records(
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
        
        # Log each quality check type
        for check_type, failures in error_types.items():
            sql = """
                INSERT INTO data_quality_checks
                    (file_id, dataset, check_type, check_name, passed, 
                     failed_count, total_count, failure_rate, sample_failures)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            failed_count = len(failures)
            total_count = self.metrics.extract_records
            failure_rate = (failed_count / total_count * 100) if total_count > 0 else 0
            
            cursor.execute(sql, (
                str(self.source_context.get('source_file')) if self.source_context.get('source_file') else None,
                self.dataset,
                check_type,
                f"{check_type}_validation",
                failed_count == 0,
                failed_count,
                total_count,
                round(failure_rate, 2),
                json.dumps(failures[:10])  # Sample first 10 failures
            ))
        
        # Overall quality check
        sql = """
            INSERT INTO data_quality_checks
                (file_id, dataset, check_type, check_name, passed, 
                 failed_count, total_count, failure_rate, sample_failures)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.execute(sql, (
            str(self.source_context.get('source_file')) if self.source_context.get('source_file') else None,
            self.dataset,
            'overall',
            'overall_validation',
            self.metrics.invalid_records == 0,
            self.metrics.invalid_records,
            self.metrics.extract_records,
            round((self.metrics.invalid_records / self.metrics.extract_records * 100) 
                  if self.metrics.extract_records > 0 else 0, 2),
            json.dumps({'summary': f'{self.metrics.valid_records} valid, {self.metrics.invalid_records} invalid'})
        ))
        
        self.connection.commit()


def run_csv_pipeline(file_path: str, connection, mapping: Dict[str, Any],
                     source_file_id: Optional[UUID] = None,
                     device_id: Optional[str] = None) -> PipelineMetrics:
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
    source_context = {
        'source_type': 'csv',
        'source_file': source_file_id,
        'device_id': device_id,
        'ingestion_method': 'batch'
    }
    
    # Extract CSV data
    raw_records = []
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        raw_records = list(reader)
    
    # Run pipeline
    pipeline = DataPipeline(connection, mapping, source_context)
    return pipeline.execute(raw_records)


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
