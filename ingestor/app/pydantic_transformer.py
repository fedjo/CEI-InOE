"""
Pydantic Transformer Module

Unified validation + transformation using Pydantic models.
Replaces the separate DataValidator and DataTransformer classes.
"""

from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from uuid import UUID
import logging

from pydantic import ValidationError as PydanticValidationError

from models import get_model_for_dataset, BaseRecord, SourceType
from validation import ValidationResult

logger = logging.getLogger(__name__)


class PydanticTransformer:
    """
    Unified validation + transformation using Pydantic models.

    This class replaces:
    - validation.py::DataValidator (schema, type, business rule validation)
    - transformation.py::DataTransformer (column mapping, type coercion)

    Benefits:
    - Single pass validation + transformation
    - Type-safe models with IDE support
    - Better error messages from Pydantic
    - Constraint validation via Field(ge=, le=, etc.)
    """

    def __init__(self, mapping: Dict[str, Any]):
        """
        Initialize transformer from YAML mapping configuration.

        Args:
            mapping: YAML mapping dict with 'dataset', 'columns', etc.
        """
        self.mapping = mapping
        self.dataset = str(mapping.get('dataset'))
        self.columns = mapping.get('columns', {})  # CSV column → DB column mapping
        self.model_class = get_model_for_dataset(self.dataset)

        logger.debug(f"PydanticTransformer initialized for {self.dataset}")

    def transform_and_validate(
        self,
        raw_data: Dict[str, Any],
        source_context: Dict[str, Any]
    ) -> Tuple[Optional[Dict[str, Any]], ValidationResult]:
        """
        Transform and validate a single record using Pydantic.

        This method:
        1. Maps CSV columns to DB columns using YAML mapping
        2. Injects source metadata
        3. Validates with Pydantic model (type coercion + constraints)
        4. Returns transformed dict or validation errors
        
        Args:
            raw_data: Raw CSV row or API response dict
            source_context: Source metadata dict with keys:
                - source_type: 'csv', 'api', 'excel'
                - source_file: UUID of ingest_file
                - device_id: Device identifier string
                - api_endpoint: API URL (if source_type == 'api')
                - ingestion_method: 'batch' or 'streaming'
        
        Returns:
            Tuple of (transformed_dict, validation_result)
            - transformed_dict is None if validation failed
            - validation_result.is_valid indicates success/failure
        """
        result = ValidationResult()
        
        # ── Step 1: Map CSV columns to DB columns ─────────────────────────
        mapped_data = {}
        for csv_col, db_col in self.columns.items():
            if csv_col in raw_data:
                mapped_data[db_col] = raw_data[csv_col]

        # ── Step 2: Inject source metadata and device_id ────────────────────────────────
        if 'energy' in self.dataset:
            # Inject device_id for energy datasets
            device_id = source_context.get('device_id')
            if device_id is not None:
                mapped_data['device_id'] = device_id  # Assuming device_id is integer FK
        # These fields are defined in BaseRecord but won't be inserted to DB
        # (they're excluded in model_dump below)
        source_type_str = source_context.get('source_type', 'csv')
        try:
            mapped_data['source_type'] = SourceType(source_type_str)
        except ValueError:
            mapped_data['source_type'] = SourceType.UNKNOWN

        mapped_data['source_file'] = source_context.get('source_file')
        mapped_data['source_api_endpoint'] = source_context.get('api_endpoint')
        mapped_data['source_device_id'] = str(source_context.get('device_id'))
        mapped_data['ingestion_method'] = source_context.get('ingestion_method', 'batch')
        mapped_data['ingested_at'] = datetime.now()

        # ── Step 3: Validate with Pydantic ────────────────────────────────
        try:
            validated_model: BaseRecord = self.model_class.model_validate(mapped_data)

            # Convert to dict for database insertion
            # Exclude source metadata fields (they're for tracking, not DB columns)
            transformed = validated_model.model_dump(
                # exclude={
                #     'source_type',
                #     'source_api_endpoint',
                #     'source_device_id',
                #     'ingestion_method',
                #     'ingested_at'
                # },
                exclude_none=False,  # Keep None values for optional fields
            )

            # Handle source_file separately - it IS a DB column for some tables
            if source_context.get('source_file'):
                # Ensure it's a UUID, not string
                sf = source_context['source_file']
                transformed['source_file'] = sf if isinstance(sf, UUID) else UUID(str(sf))
            else:
                # Remove if None to avoid inserting NULL
                transformed.pop('source_file', None)

            return transformed, result
            
        except PydanticValidationError as e:
            # ── Step 4: Convert Pydantic errors to ValidationResult ───────
            for error in e.errors():
                # Build field path (handles nested fields)
                field = '.'.join(str(loc) for loc in error['loc'])
                
                # Get the input value that caused the error
                input_value = error.get('input')
                
                # Map Pydantic error types to our error types
                pydantic_type = error['type']
                error_type = self._map_error_type(pydantic_type)
                
                # Build constraint info for debugging
                constraint = error.get('ctx', {})
                
                result.add_error(
                    field=field,
                    value=input_value,
                    error_type=error_type,
                    message=error['msg'],
                    constraint=constraint if constraint else None
                )
            
            return None, result
    
    def _map_error_type(self, pydantic_type: str) -> str:
        """
        Map Pydantic error types to our validation error types.
        
        Args:
            pydantic_type: Pydantic error type string
        
        Returns:
            Mapped error type for ValidationResult
        """
        type_mapping = {
            'missing': 'missing',
            'value_error': 'type',
            'type_error': 'type',
            'int_parsing': 'type',
            'float_parsing': 'type',
            'datetime_parsing': 'type',
            'date_parsing': 'type',
            'greater_than_equal': 'range',
            'less_than_equal': 'range',
            'greater_than': 'range',
            'less_than': 'range',
            'string_pattern_mismatch': 'pattern',
            'enum': 'allowed_values',
            'literal_error': 'allowed_values',
        }
        
        # Check for partial matches (Pydantic types can be complex)
        for key, mapped in type_mapping.items():
            if key in pydantic_type.lower():
                return mapped
        
        return 'validation'  # Generic fallback
    
    def transform_batch(
        self,
        raw_records: List[Dict[str, Any]],
        source_context: Dict[str, Any]
    ) -> Tuple[List[Dict[str, Any]], List[Tuple[int, ValidationResult]]]:
        """
        Transform and validate multiple records.
        
        Args:
            raw_records: List of raw data dicts
            source_context: Source metadata dict
        
        Returns:
            Tuple of:
            - valid_records: List of transformed dicts ready for DB
            - invalid_records: List of (row_index, ValidationResult) tuples
        """
        valid_records = []
        invalid_records = []
        
        for idx, raw_record in enumerate(raw_records):
            transformed, result = self.transform_and_validate(raw_record, source_context)
            
            if result.is_valid and transformed is not None:
                # Add row index for tracking
                transformed['_row_index'] = idx
                valid_records.append(transformed)
            else:
                invalid_records.append((idx, result))
        
        logger.info(
            f"Batch transform: {len(valid_records)} valid, "
            f"{len(invalid_records)} invalid out of {len(raw_records)} total"
        )
        
        return valid_records, invalid_records
    
    def validate_record(self, data: Dict[str, Any]) -> ValidationResult:
        """
        Validate a record without transformation (compatibility method).
        
        This matches the signature of DataValidator.validate_record()
        for backward compatibility with pipeline.py.
        
        Args:
            data: Already-mapped data dict
        
        Returns:
            ValidationResult with is_valid and errors
        """
        # For this method, we assume data is already mapped
        # Just need to validate with Pydantic
        result = ValidationResult()
        
        try:
            self.model_class.model_validate(data)
            return result  # is_valid = True by default
            
        except PydanticValidationError as e:
            for error in e.errors():
                field = '.'.join(str(loc) for loc in error['loc'])
                result.add_error(
                    field=field,
                    value=error.get('input'),
                    error_type=self._map_error_type(error['type']),
                    message=error['msg'],
                    constraint=error.get('ctx')
                )
            return result


class PydanticTransformerFactory:
    """
    Factory for creating PydanticTransformer instances.
    Caches transformers by dataset for efficiency.
    """
    
    _cache: Dict[str, PydanticTransformer] = {}
    
    @classmethod
    def get_transformer(cls, mapping: Dict[str, Any]) -> PydanticTransformer:
        """
        Get or create a PydanticTransformer for a mapping.
        
        Args:
            mapping: YAML mapping configuration
        
        Returns:
            PydanticTransformer instance
        """
        dataset = str(mapping.get('dataset'))
        
        if dataset not in cls._cache:
            cls._cache[dataset] = PydanticTransformer(mapping)
        
        return cls._cache[dataset]
    
    @classmethod
    def clear_cache(cls):
        """Clear the transformer cache."""
        cls._cache.clear()
