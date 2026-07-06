"""
DelPro Milking Export Preprocessor.

Transforms DelPro milking parlour export files into normalized daily records
suitable for the dairy_production pipeline.

Input Format:
- Tab-separated file with numeric field IDs in first row
- Header row with column names starting with "Cow no"
- Data rows with cow ID and milk production for today/yesterday sessions
- Missing values represented as "????" or "??????"
- Trailer row "ZN" marks end of data

Business Rules:
- Use only today's milk data (Milk 1/2/3 today columns)
- Active cow = all three today milk values present (not missing)
- Average milk per cow = total today milk / active cow count
- Production date = system current date at ingestion time

Output: Single row per file with columns matching dairy_production mapping:
- Date: production date (YYYY-MM-DD)
- Day production/cow (kg): average milk yield per active cow
- Nr. animals: count of active cows
"""

import logging
from datetime import date
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def is_missing(value: Any) -> bool:
    """Check if a value represents missing data."""
    if value is None:
        return True
    value_str = str(value).strip()
    return not value_str or value_str.startswith('?')


def parse_float(value: Any) -> Optional[float]:
    """Parse a value as float, returning None if missing or invalid."""
    if is_missing(value):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def is_delpro_format(columns: List[str]) -> bool:
    """
    Detect DelPro milking export format by column structure.
    
    Expected columns (case-insensitive):
    - Cow no
    - Milk 1 today, Milk 2 today, Milk 3 today
    - Milk 1 yesterday, Milk 2 yesterday, Milk 3 yesterday
    - Duration columns (optional)
    """
    if not columns or len(columns) < 7:
        return False
    
    col_lower = [str(c).lower().strip() for c in columns]
    
    # Check for required columns
    has_cow_no = 'cow no' in col_lower or 'cow_no' in col_lower
    has_milk_today = any('milk' in c and 'today' in c for c in col_lower)
    has_milk_yesterday = any('milk' in c and 'yesterday' in c for c in col_lower)
    
    return has_cow_no and has_milk_today and has_milk_yesterday


def preprocess_delpro_milking(
    raw_content: List[Dict[str, Any]],
    production_date_override: Optional[date] = None
) -> List[Dict[str, Any]]:
    """
    Preprocess DelPro milking export data.
    
    Computes active cow count and average milk production per cow
    from today's milking sessions only.
    
    Args:
        raw_content: Raw rows from pandas read_excel/read_csv
        production_date_override: Optional production date for testing;
                                  defaults to current system date
        
    Returns:
        List with single normalized daily record:
        - Date: production date (YYYY-MM-DD)
        - Day production/cow (kg): average milk per active cow
        - Nr. animals: active cow count
        
        Returns empty list if no active cows found.
    """
    if not raw_content:
        logger.warning("Empty raw content for DelPro milking preprocessor")
        return []
    
    # Use current date if not overridden
    prod_date = production_date_override or date.today()
    
    # Detect column indices
    first_row = raw_content[0]
    columns = list(first_row.keys())
    
    col_map = _build_column_map(columns)
    if not col_map:
        logger.error("Could not identify DelPro milking column structure")
        return []
    
    # Process rows
    active_cow_count = 0
    total_milk_today = 0.0
    
    for row in raw_content:
        # Skip header and trailer rows
        cow_id = str(row.get(columns[col_map['cow_no']], '')).strip()
        if not cow_id or cow_id.upper() in ('COW NO', 'ZN', ''):
            continue
        
        # Extract today's three milk sessions
        milk_1 = parse_float(row.get(columns[col_map['milk_1_today']]))
        milk_2 = parse_float(row.get(columns[col_map['milk_2_today']]))
        milk_3 = parse_float(row.get(columns[col_map['milk_3_today']]))
        
        # Active cow must have all three today sessions present
        if milk_1 is not None and milk_2 is not None and milk_3 is not None:
            active_cow_count += 1
            total_milk_today += milk_1 + milk_2 + milk_3
    
    # Return empty if no active cows (would fail validation anyway)
    if active_cow_count == 0:
        logger.warning(
            "No active cows found in DelPro milking export "
            "(requires all three today milk sessions present)"
        )
        return []
    
    # Compute average milk per cow
    avg_milk_per_cow = total_milk_today / active_cow_count
    
    logger.info(
        f"DelPro milking preprocessor: {active_cow_count} active cows, "
        f"{total_milk_today:.2f} kg total, "
        f"{avg_milk_per_cow:.2f} kg/cow average"
    )
    
    # Return single normalized record
    return [{
        'Date': prod_date.strftime('%Y-%m-%d'),
        'Day production/cow (kg)': round(avg_milk_per_cow, 2),
        'Nr. animals': active_cow_count,
    }]


def _build_column_map(columns: List[str]) -> Dict[str, int]:
    """
    Build a map of semantic column names to indices.
    
    Expected columns:
    0: Cow no
    1: Milk 1 today
    2: Milk 2 today
    3: Milk 3 today
    4: Milk 1 yesterday
    5: Milk 2 yesterday
    6: Milk 3 yesterday
    7+: Duration columns (optional)
    """
    col_map = {}
    
    for i, col in enumerate(columns):
        col_lower = str(col).lower().strip()
        
        if 'cow' in col_lower and 'no' in col_lower:
            col_map['cow_no'] = i
        elif 'milk 1' in col_lower and 'today' in col_lower:
            col_map['milk_1_today'] = i
        elif 'milk 2' in col_lower and 'today' in col_lower:
            col_map['milk_2_today'] = i
        elif 'milk 3' in col_lower and 'today' in col_lower:
            col_map['milk_3_today'] = i
    
    # Validate we have required columns
    required = ['cow_no', 'milk_1_today', 'milk_2_today', 'milk_3_today']
    if not all(k in col_map for k in required):
        logger.warning(
            f"Missing required DelPro columns. Found: {list(col_map.keys())}"
        )
        return {}
    
    return col_map
