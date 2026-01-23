"""
Data Transformation Module
Unified transformation logic with source context support.
Replaces the duplicate coerce/coerce_value functions.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class TransformationError(Exception):
    """Raised when transformation fails."""
    pass


class DataTransformer:
    """Handles data transformation with type coercion."""

    def __init__(self, mapping: Dict[str, Any]):
        """
        Initialize from YAML mapping configuration.

        mapping structure:
        {
            'columns': {'csv_col': 'db_col'},
            'coercions': {'db_col': 'float'},
            'transformations': [...]  # Optional custom transformations
        }
        """
        self.mapping = mapping
        self.columns = mapping.get('columns', {})
        self.coercions = mapping.get('coercions', {})
        self.transformations = mapping.get('transformations', [])

    def transform_record(self, raw_data: Dict[str, Any],
                        source_context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform a single record from source to target format.

        Args:
            raw_data: Raw CSV row or API response
            source_context: Context about data source (type, file_id, device_id, etc.)

        Returns:
            Transformed record ready for database insertion
        """
        transformed = {}

        # Map and coerce data columns
        for source_col, target_col in self.columns.items():
            if source_col in raw_data:
                raw_value = raw_data[source_col]
                coercion_type = self.coercions.get(target_col, 'str')

                try:
                    transformed[target_col] = self.coerce_value(raw_value, coercion_type)
                except Exception as e:
                    logger.warning(
                        f"Coercion failed for {target_col}: {raw_value} -> {coercion_type}: {e}"
                    )
                    transformed[target_col] = None
        
        # Apply custom transformations if defined
        for transformation in self.transformations:
            transformed = self._apply_transformation(transformed, transformation)
        
        # Add source metadata
        transformed.update(self._build_source_metadata(source_context))

        return transformed
    
    def transform_batch(self, raw_records: List[Dict[str, Any]], 
                       source_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform multiple records."""
        return [
            self.transform_record(record, source_context) 
            for record in raw_records
        ]
    
    def coerce_value(self, value: Any, target_type: str) -> Any:
        """
        Unified coercion function - replaces both coerce() and coerce_value().
        
        Raises TransformationError instead of silently returning None.
        """
        # Handle None/empty
        if value is None or value == '' or (isinstance(value, float) and pd.isna(value)):
            return None
        
        try:
            if target_type == 'str':
                return str(value).strip()
            
            elif target_type == 'int':
                # Handle "123.0" string case
                if isinstance(value, str):
                    value = value.strip()
                return int(float(value))
            
            elif target_type == 'float':
                if isinstance(value, str):
                    value = value.replace(",", ".")
                return float(value)
            
            elif target_type == 'bool':
                if isinstance(value, bool):
                    return value
                if isinstance(value, str):
                    value_lower = value.lower()
                    if value_lower in ('true', '1', 'yes', 'y'):
                        return True
                    if value_lower in ('false', '0', 'no', 'n'):
                        return False
                return bool(value)
            
            elif target_type == 'datetime':
                if isinstance(value, datetime):
                    return value
                if isinstance(value, pd.Timestamp):
                    return value.to_pydatetime()
                if isinstance(value, str) and any(token in value.lower() for token in ('am', 'pm')):
                    return datetime.strptime(value, '%m/%d/%Y %H:%M:%S %p')
                # Try parsing ISO format
                return datetime.fromisoformat(str(value))

            elif target_type == 'date':
                if hasattr(value, 'date'):
                    return value.date()
                if isinstance(value, str) and any(token in value.lower() for token in ('am', 'pm')):
                    # This handles strings with AM/PM read from `CSVReader` objects
                    # In the future we may want to generalize this further and use `read_csv` parsing
                    return datetime.strptime(value, '%m/%d/%Y %H:%M:%S %p').date()

                # This handles np.datetime64 and ISO strings from `read_csv` and `read_excel`
                return datetime.fromisoformat(str(value)).date()

            elif target_type == 'time':
                if hasattr(value, 'time'):
                    return value.time()
                return datetime.fromisoformat(str(value)).time()

            else:
                raise TransformationError(f"Unsupported coercion type: {target_type}")
        
        except Exception as e:
            raise TransformationError(
                f"Cannot coerce '{value}' to {target_type}: {str(e)}"
            )
    
    def _apply_transformation(self, data: Dict[str, Any],
                            transformation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply custom transformation logic.
        
        transformation format:
        {
            'type': 'expression',
            'target_field': 'new_field',
            'expression': 'field1 + field2'
        }
        """
        trans_type = transformation.get('type')
        
        if trans_type == 'expression':
            # Simple expression evaluation
            target = transformation['target_field']
            expr = transformation['expression']
            try:
                # Safe eval with limited scope
                data[target] = eval(expr, {"__builtins__": {}}, data)
            except Exception as e:
                logger.warning(f"Transformation expression failed: {expr}: {e}")
        
        elif trans_type == 'lookup':
            # Lookup table transformation
            source_field = transformation['source_field']
            target_field = transformation['target_field']
            lookup_table = transformation['lookup_table']
            default = transformation.get('default')
            
            value = data.get(source_field)
            data[target_field] = lookup_table.get(value, default)
        
        elif trans_type == 'concat':
            # Concatenate fields
            fields = transformation['fields']
            target = transformation['target_field']
            separator = transformation.get('separator', ' ')
            
            values = [str(data.get(f, '')) for f in fields]
            data[target] = separator.join(values)
        
        return data
    
    def _build_source_metadata(self, source_context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build source metadata fields from context.
        
        source_context format:
        {
            'source_type': 'csv' or 'api',
            'source_file': UUID,
            'device_id': 'device_123',
            'ingestion_method': 'batch' or 'streaming',
            'api_endpoint': 'https://...' (optional)
        }
        """
        metadata = {}
        
        # Core metadata fields
        metadata['source_type'] = source_context.get('source_type', 'unknown')
        metadata['source_device_id'] = source_context.get('device_id')
        metadata['ingestion_method'] = source_context.get('ingestion_method', 'batch')
        
        # Source-specific fields
        if source_context.get('source_type') == 'csv':
            metadata['source_file'] = source_context.get('source_file')
            metadata['source_api_endpoint'] = None
        
        elif source_context.get('source_type') == 'api':
            metadata['source_file'] = None
            metadata['source_api_endpoint'] = source_context.get('api_endpoint')
        
        else:
            # Hybrid or unknown - include both
            metadata['source_file'] = source_context.get('source_file')
            metadata['source_api_endpoint'] = source_context.get('api_endpoint')
        
        # Timestamp
        metadata['ingested_at'] = datetime.now()
        
        return metadata


class LegacyTransformationAdapter:
    """
    Adapter to maintain backward compatibility with existing code.
    Wraps new DataTransformer to work with legacy function signatures.
    """
    
    @staticmethod
    def transform_row_csv(csv_row: Dict[str, str], 
                         mapping: Dict[str, Any]) -> Dict[str, Any]:
        """
        Legacy CSV transformation (from csv.DictReader).
        Maintains compatibility with existing code.
        """
        transformer = DataTransformer(mapping)
        
        # Build minimal source context (CSV assumed)
        source_context = {
            'source_type': 'csv',
            'ingestion_method': 'batch'
        }
        
        return transformer.transform_record(csv_row, source_context)
    
    @staticmethod
    def transform_row_pandas(row: pd.Series, 
                           mapping: Dict[str, Any]) -> Dict[str, Any]:
        """
        Legacy pandas transformation.
        Maintains compatibility with process_file().
        """
        # Convert pandas Series to dict
        csv_row = row.to_dict()
        
        transformer = DataTransformer(mapping)
        
        # Build minimal source context
        source_context = {
            'source_type': 'csv',
            'ingestion_method': 'batch'
        }
        
        return transformer.transform_record(csv_row, source_context)


# Convenience function for backward compatibility
def coerce_value(value: Any, coercion_type: str) -> Any:
    """
    Standalone coercion function for backward compatibility.
    Uses DataTransformer internally.
    """
    transformer = DataTransformer({})
    try:
        return transformer.coerce_value(value, coercion_type)
    except TransformationError:
        return None  # Legacy behavior: silent failure
