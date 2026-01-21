"""
Data Validation Module
Separates validation from transformation with clear error reporting.
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime
import re


@dataclass
class ValidationError:
    """Represents a single validation error."""
    field: str
    value: Any
    error_type: str  # 'missing', 'type', 'range', 'pattern', 'business_rule'
    message: str
    constraint: Optional[Any] = None


@dataclass
class ValidationResult:
    """Collects all validation results for a record."""
    is_valid: bool = True
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def add_error(self, field: str, value: Any, error_type: str,
                  message: str, constraint: Any = None):
        """Add a validation error."""
        self.errors.append(ValidationError(
            field=field,
            value=value,
            error_type=error_type,
            message=message,
            constraint=constraint
        ))
        self.is_valid = False

    def add_warning(self, message: str):
        """Add a validation warning (non-fatal)."""
        self.warnings.append(message)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSONB storage."""
        return {
            'is_valid': self.is_valid,
            'errors': [
                {
                    'field': e.field,
                    'value': str(e.value) if e.value is not None else None,
                    'error_type': e.error_type,
                    'message': e.message,
                    'constraint': e.constraint
                }
                for e in self.errors
            ],
            'warnings': self.warnings
        }


class SchemaValidator:
    """Validates data against schema definition."""

    def __init__(self, schema: Dict[str, Any]):
        """
        Initialize with schema from YAML mapping.

        schema format:
        {
            'field_name': {
                'type': 'float',
                'required': True,
                'constraints': {...}
            }
        }
        """
        self.schema = schema

    def validate(self, data: Dict[str, Any]) -> ValidationResult:
        """Validate data against schema."""
        result = ValidationResult()
        
        # Check required fields
        for field, rules in self.schema.items():
            if rules.get('required', False):
                if field not in data or data[field] is None or data[field] == '':
                    result.add_error(
                        field=field,
                        value=data.get(field),
                        error_type='missing',
                        message=f"Required field '{field}' is missing or empty"
                    )

        # Check for unexpected fields (warning only)
        schema_fields = set(self.schema.keys())
        data_fields = set(data.keys())
        unexpected = data_fields - schema_fields
        if unexpected:
            result.add_warning(f"Unexpected fields found: {', '.join(unexpected)}")
        
        return result


class TypeValidator:
    """Validates and coerces data types."""
    
    SUPPORTED_TYPES = {
        'str', 'int', 'float', 'bool', 'datetime', 'date', 'time'
    }
    
    def __init__(self, type_mapping: Dict[str, str]):
        """
        Initialize with type mappings from YAML.

        type_mapping format:
        {
            'field_name': 'float',
            'timestamp': 'datetime'
        }
        """
        self.type_mapping = type_mapping
    
    def validate(self, data: Dict[str, Any]) -> ValidationResult:
        """Validate data types can be coerced."""
        result = ValidationResult()

        for field, expected_type in self.type_mapping.items():
            if field not in data:
                continue  # Schema validator handles missing fields
            
            value = data[field]

            # Skip None values (handled by required check)
            if value is None or value == '':
                continue
            
            # Attempt type validation
            try:
                self._validate_type(value, expected_type)
            except (ValueError, TypeError) as e:
                result.add_error(
                    field=field,
                    value=value,
                    error_type='type',
                    message=f"Cannot convert '{value}' to {expected_type}: {str(e)}",
                    constraint={'expected_type': expected_type}
                )
        
        return result
    
    def _validate_type(self, value: Any, expected_type: str):
        """Validate a single value can be coerced to expected type."""
        if expected_type not in self.SUPPORTED_TYPES:
            raise ValueError(f"Unsupported type: {expected_type}")
        
        # String - always valid
        if expected_type == 'str':
            str(value)
            return
        
        # Integer
        if expected_type == 'int':
            int(float(value))  # Handle "123.0" strings
            return
        
        # Float
        if expected_type == 'float':
            float(value)
            return
        
        # Boolean
        if expected_type == 'bool':
            if isinstance(value, bool):
                return
            if isinstance(value, str):
                if value.lower() in ('true', '1', 'yes', 'y'):
                    return
                if value.lower() in ('false', '0', 'no', 'n'):
                    return
            raise ValueError(f"Cannot convert {value} to boolean")
        
        # Datetime
        if expected_type == 'datetime':
            if isinstance(value, datetime):
                return
            # Try common formats
            datetime.fromisoformat(str(value))
            return
        
        # Date
        if expected_type == 'date':
            if hasattr(value, 'date'):
                return
            datetime.strptime(str(value), '%Y-%m-%d')
            return
        
        # Time
        if expected_type == 'time':
            datetime.strptime(str(value), '%H:%M:%S')
            return


class BusinessRuleValidator:
    """Validates business rules and constraints."""
    
    def __init__(self, rules: Dict[str, Any]):
        """
        Initialize with validation rules from YAML.
        
        rules format:
        {
            'field_name': {
                'min': 0,
                'max': 100,
                'pattern': r'^\d{3}-\d{4}$',
                'allowed_values': ['A', 'B', 'C']
            }
        }
        """
        self.rules = rules
    
    def validate(self, data: Dict[str, Any]) -> ValidationResult:
        """Validate business rules."""
        result = ValidationResult()
        
        for field, constraints in self.rules.items():
            if field not in data:
                continue
            
            value = data[field]
            
            # Skip None values
            if value is None or value == '':
                continue
            
            # Min/Max range validation
            if 'min' in constraints:
                try:
                    if float(value) < float(constraints['min']):
                        result.add_error(
                            field=field,
                            value=value,
                            error_type='range',
                            message=f"Value {value} is below minimum {constraints['min']}",
                            constraint={'min': constraints['min']}
                        )
                except (ValueError, TypeError):
                    pass  # Type validator will catch this
            
            if 'max' in constraints:
                try:
                    if float(value) > float(constraints['max']):
                        result.add_error(
                            field=field,
                            value=value,
                            error_type='range',
                            message=f"Value {value} exceeds maximum {constraints['max']}",
                            constraint={'max': constraints['max']}
                        )
                except (ValueError, TypeError):
                    pass
            
            # Pattern validation (regex)
            if 'pattern' in constraints:
                pattern = constraints['pattern']
                if not re.match(pattern, str(value)):
                    result.add_error(
                        field=field,
                        value=value,
                        error_type='pattern',
                        message=f"Value '{value}' does not match pattern '{pattern}'",
                        constraint={'pattern': pattern}
                    )
            
            # Allowed values (enum)
            if 'allowed_values' in constraints:
                allowed = constraints['allowed_values']
                if value not in allowed:
                    result.add_error(
                        field=field,
                        value=value,
                        error_type='allowed_values',
                        message=f"Value '{value}' not in allowed values: {allowed}",
                        constraint={'allowed_values': allowed}
                    )
            
            # String length validation
            if 'min_length' in constraints:
                if len(str(value)) < constraints['min_length']:
                    result.add_error(
                        field=field,
                        value=value,
                        error_type='length',
                        message=f"Value too short (min: {constraints['min_length']})",
                        constraint={'min_length': constraints['min_length']}
                    )
            
            if 'max_length' in constraints:
                if len(str(value)) > constraints['max_length']:
                    result.add_error(
                        field=field,
                        value=value,
                        error_type='length',
                        message=f"Value too long (max: {constraints['max_length']})",
                        constraint={'max_length': constraints['max_length']}
                    )
        
        return result


class DataValidator:
    """Main validator that orchestrates all validation types."""
    
    def __init__(self, mapping: Dict[str, Any]):
        """
        Initialize from YAML mapping configuration.
        
        Expected mapping structure:
        {
            'dataset': 'environmental_metrics',
            'columns': {...},
            'coercions': {...},
            'validation_rules': {
                'schema': {...},
                'constraints': {...}
            }
        }
        """
        self.mapping = mapping
        
        # Extract validation configuration
        validation_config = mapping.get('validation_rules', {})
        
        # Build schema from columns and coercions
        schema = {}
        for field in mapping.get('columns', {}).values():
            schema[field] = {
                'type': mapping.get('coercions', {}).get(field, 'str'),
                'required': field in validation_config.get('schema', {}).get('required', [])
            }
        
        # Initialize validators
        self.schema_validator = SchemaValidator(schema)
        self.type_validator = TypeValidator(mapping.get('coercions', {}))
        self.business_validator = BusinessRuleValidator(
            validation_config.get('constraints', {})
        )
    
    def validate_record(self, data: Dict[str, Any]) -> ValidationResult:
        """
        Validate a single record through all validation stages.
        
        Returns combined ValidationResult.
        """
        # Stage 1: Schema validation
        schema_result = self.schema_validator.validate(data)
        if not schema_result.is_valid:
            return schema_result  # Fail fast on schema errors
        
        # Stage 2: Type validation
        type_result = self.type_validator.validate(data)
        
        # Stage 3: Business rules validation
        business_result = self.business_validator.validate(data)
        
        # Combine results
        combined = ValidationResult()
        combined.errors.extend(schema_result.errors)
        combined.errors.extend(type_result.errors)
        combined.errors.extend(business_result.errors)
        combined.warnings.extend(schema_result.warnings)
        combined.warnings.extend(type_result.warnings)
        combined.warnings.extend(business_result.warnings)
        combined.is_valid = len(combined.errors) == 0
        
        return combined
    
    def validate_batch(self, records: List[Dict[str, Any]]) -> List[ValidationResult]:
        """Validate multiple records."""
        return [self.validate_record(record) for record in records]
