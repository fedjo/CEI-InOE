"""
Pipeline runner that processes InputEnvelopes.
"""

import logging
import uuid
from typing import Optional

import psycopg2
import yaml

from connectors.base import InputEnvelope
from pipeline import DataPipeline, PipelineMetrics

logger = logging.getLogger(__name__)


class DuplicateInputError(Exception):
    """Raised when input was already processed."""
    pass


class PipelineRunner:
    """Processes InputEnvelopes through the pipeline."""
    
    def __init__(self, db_dsn: str):
        self.db_dsn = db_dsn
    
    def run(self, envelope: InputEnvelope) -> PipelineMetrics:
        """Execute pipeline for envelope."""
        conn = psycopg2.connect(self.db_dsn)

        try:
            # Check duplicates
            if self._is_duplicate(conn, envelope):
                raise DuplicateInputError(envelope.input_id)
            
            # Load mapping
            mapping = self._load_mapping(envelope.hint_mapping)
            if not mapping:
                raise ValueError(f"No mapping for {envelope.source_uri}")
            
            # Resolve device
            device_id = self._resolve_device_id(conn, envelope.hint_device_id)
            
            # Register input
            file_id = self._register_input(conn, envelope, device_id)
            
            # Build context
            source_context = {
                'source_type': envelope.metadata.get('source_type'),
                'source_file': file_id,
                'source_api_endpoint': envelope.source_uri,
                'device_id': device_id,
                'ingestion_method': envelope.content_type,
            }

            # Run pipeline
            pipeline = DataPipeline(conn, mapping, source_context)
            metrics = pipeline.execute(envelope.content)
            
            # Update record
            self._update_record(conn, file_id, metrics)
            
            return metrics
            
        except DuplicateInputError:
            raise
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def _is_duplicate(self, conn, envelope: InputEnvelope) -> bool:
        """Check if already processed."""
        sha256 = envelope.metadata.get('sha256')
        if not sha256:
            return False
        
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM ingest_file WHERE sha256 = %s", (sha256,))
        return cursor.fetchone() is not None
    
    def _load_mapping(self, path: Optional[str]) -> Optional[dict]:
        """Load YAML mapping."""
        if not path:
            return None
        try:
            with open(path) as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load mapping {path}: {e}")
            return None
    
    def _resolve_device_id(self, conn, device_id: Optional[str]) -> Optional[int]:
        """Resolve device string to DB ID."""
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM generic_device WHERE device_id = %s", (device_id or 'unknown',))
        row = cursor.fetchone()
        return row[0] if row else None
    
    def _register_input(self, conn, envelope: InputEnvelope, device_id: Optional[int]) -> uuid.UUID:
        """Register input in database."""
        file_id = uuid.uuid4()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO ingest_file 
                (file_id, file_name, device_id, granularity, start_date, end_date, sha256, pipeline_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s, '3.0')
        """, (
            str(file_id),
            envelope.metadata.get('file_name', envelope.source_uri),
            str(device_id) if device_id else None,
            envelope.hint_granularity,
            envelope.metadata.get('start_date'),
            envelope.metadata.get('end_date'),
            envelope.metadata.get('sha256'),
        ))
        conn.commit()
        
        return file_id
    
    def _update_record(self, conn, file_id: uuid.UUID, metrics: PipelineMetrics) -> None:
        """Update record with results."""
        cursor = conn.cursor()
        
        quality = (
            round(metrics.valid_records / metrics.extract_records * 100, 2)
            if metrics.extract_records > 0 else 0
        )
        
        cursor.execute("""
            UPDATE ingest_file
            SET execution_time_ms = %s, validation_status = %s, quality_score = %s
            WHERE file_id = %s
        """, (
            int(metrics.total_duration * 1000),
            'passed' if metrics.invalid_records == 0 else 'partial',
            quality,
            str(file_id),
        ))
        conn.commit()
