"""
Preprocessors for special file formats.

Each preprocessor transforms raw file data into a normalized format
that can be processed by the standard pipeline.
"""

from .delaval import preprocess_delaval
from .delpro import preprocess_delpro_milking

__all__ = ['preprocess_delaval', 'preprocess_delpro_milking']
