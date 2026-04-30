"""
Delaval Milking Performance Parlour File Preprocessor.

Transforms Delaval's unusual file format into normalized daily records
suitable for the dairy_production pipeline.

Delaval Format Characteristics:
- Header row: "Parlour Name  Parlour1,,,..."
- Date rows: "Date  DD/MM/YYYY,,,..."  
- Session rows: "1,03:57:01,18,05:56:45,..." (sessions 1, 2, 3 per day)
- Totals rows: ",T,55,T,HHt:mm,772,..." (daily totals, marked with T)
- Grand totals at bottom (skip these)

Output: One row per day with columns matching dairy_production mapping.
"""

import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def preprocess_delaval(
    raw_content: List[Dict[str, Any]],
    parlour_name: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Preprocess Delaval milking performance data.
    
    Extracts daily totals rows and normalizes them for dairy_production.
    
    Args:
        raw_content: Raw rows from pandas read_csv/read_excel
        parlour_name: Optional parlour name override
        
    Returns:
        List of normalized daily records with columns:
        - Date: production date (YYYY-MM-DD)
        - day_production_per_cow_kg: milk yield per cow
        - number_of_animals: total cows milked
        - parlour_name: parlour identifier
    """
    if not raw_content:
        return []
    
    # Get column names from first row's keys
    first_row = raw_content[0]
    columns = list(first_row.keys())
    
    # Find key column indices by name
    col_map = _build_column_map(columns)
    
    if not col_map:
        logger.warning("Could not identify Delaval column structure")
        return []
    
    results = []
    current_date = None
    detected_parlour = parlour_name
    session_yields = []  # Accumulate yield per cow from each session
    session_cows = []    # Accumulate cow counts from each session
    
    for row in raw_content:
        row_values = [row.get(col) for col in columns]
        
        # Try to detect parlour name from header rows
        if detected_parlour is None:
            detected_parlour = _extract_parlour_name(row_values)
        
        # Check if this is a date row
        date_match = _extract_date(row_values)
        if date_match:
            current_date = date_match
            # Reset session accumulators for new date
            session_yields = []
            session_cows = []
            continue
        
        # Check if this is a session row (numbered 1, 2, 3, etc.)
        if _is_session_row(row_values, col_map):
            # Accumulate session yield and cow count
            if 'milk_yield_per_cow' in col_map:
                yield_val = _parse_european_number(row_values[col_map['milk_yield_per_cow']])
                if yield_val is not None:
                    session_yields.append(yield_val)
            if 'total_cows' in col_map:
                cows_val = _parse_european_number(row_values[col_map['total_cows']])
                if cows_val is not None:
                    session_cows.append(cows_val)
            continue
        
        # Check if this is a totals row (marked with 'T')
        if _is_totals_row(row_values, col_map):
            if current_date is None:
                logger.debug("Skipping totals row without preceding date")
                continue
            
            # Skip grand totals (very large numbers or at end)
            if _is_grand_totals(row_values, col_map):
                logger.debug("Skipping grand totals row")
                continue
            
            # Extract daily totals
            daily_record = _extract_daily_record(
                row_values, 
                col_map, 
                current_date,
                detected_parlour or "unknown",
                session_yields,
                session_cows
            )
            
            if daily_record:
                results.append(daily_record)
            
            # Reset session accumulators after processing totals
            session_yields = []
            session_cows = []
    
    logger.info(f"Delaval preprocessor: {len(results)} daily records extracted")
    return results


def _build_column_map(columns: List[str]) -> Dict[str, int]:
    """
    Build a map of semantic column names to indices.
    
    Delaval columns (in order):
    0: Session Number
    1: Milk Start Time  
    2: Number of Batches
    3: Milk End Time
    4: Milk Session Duration
    5: Total Cows
    6: Number of Milk Weights
    7: Unknown Milk Weights
    8: Total yield from unknown animals only
    9: Milk Yield
    10: Avg. Milk Duration
    11: Milk Yield per Hour
    12: Milk Yield per Cow
    13: Milk Weights per Hour
    14: Cows per Hour
    15: Cows Identified
    16: Cows Not Identified
    17: Transponders Identified
    18: Unknown Transponders
    """
    col_map = {}
    
    for i, col in enumerate(columns):
        col_lower = str(col).lower().strip()
        
        if 'session number' in col_lower:
            col_map['session_number'] = i
        elif col_lower == 'total cows':
            col_map['total_cows'] = i
        elif col_lower == 'milk yield' and 'per' not in col_lower and 'hour' not in col_lower:
            col_map['milk_yield'] = i
        elif 'milk yield per cow' in col_lower or col_lower == 'milk yield per cow':
            col_map['milk_yield_per_cow'] = i
        elif 'cows identified' in col_lower and 'not' not in col_lower:
            col_map['cows_identified'] = i
    
    # Validate we have minimum required columns
    required = ['session_number', 'total_cows', 'milk_yield_per_cow']
    if not all(k in col_map for k in required):
        # Try positional fallback for Delaval's known structure
        if len(columns) >= 13:
            col_map = {
                'session_number': 0,
                'total_cows': 5,
                'milk_yield': 9,
                'milk_yield_per_cow': 12,
                'cows_identified': 15 if len(columns) > 15 else None,
            }
            # Remove None entries
            col_map = {k: v for k, v in col_map.items() if v is not None}
            logger.debug("Using positional column mapping for Delaval")
    
    return col_map


def _extract_parlour_name(row_values: List[Any]) -> Optional[str]:
    """Extract parlour name from header row like 'Parlour Name  Parlour1'."""
    if not row_values:
        return None
    
    first_val = str(row_values[0]) if row_values[0] else ""
    
    if 'parlour name' in first_val.lower():
        # Extract the actual name after "Parlour Name"
        match = re.search(r'parlour\s*name\s+(\S+)', first_val, re.IGNORECASE)
        if match:
            return match.group(1)
    
    return None


def _extract_date(row_values: List[Any]) -> Optional[str]:
    """
    Extract date from a date row like 'Date  27/03/2026'.
    
    Returns date as ISO format string (YYYY-MM-DD).
    """
    if not row_values:
        return None
    
    first_val = str(row_values[0]) if row_values[0] else ""
    
    # Pattern: "Date  DD/MM/YYYY"
    match = re.match(r'date\s+(\d{1,2})/(\d{1,2})/(\d{4})', first_val, re.IGNORECASE)
    if match:
        day, month, year = match.groups()
        try:
            dt = datetime(int(year), int(month), int(day))
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            logger.warning(f"Invalid date: {first_val}")
            return None
    
    return None


def _is_session_row(row_values: List[Any], col_map: Dict[str, int]) -> bool:
    """Check if row is a session row (session_number is a digit 1, 2, 3, etc.)."""
    if 'session_number' not in col_map:
        return False
    
    session_val = row_values[col_map['session_number']]
    if session_val is None:
        return False
    
    session_str = str(session_val).strip()
    return session_str.isdigit() and int(session_str) > 0


def _is_totals_row(row_values: List[Any], col_map: Dict[str, int]) -> bool:
    """Check if row is a daily totals row (session_number is empty or contains 'T')."""
    if 'session_number' not in col_map:
        return False
    
    session_val = row_values[col_map['session_number']]
    
    # Totals rows have empty session number or just whitespace
    # The 'T' appears in other columns (like Number of Batches)
    if session_val is None or str(session_val).strip() == '':
        return True
    
    # Some formats may have 'T' in session number
    if str(session_val).strip().upper() == 'T':
        return True
    
    return False


def _is_grand_totals(row_values: List[Any], col_map: Dict[str, int]) -> bool:
    """
    Check if row is grand totals (sum of all days).
    
    Grand totals typically have very large cow counts (thousands)
    with European format like "23,441".
    """
    if 'total_cows' not in col_map:
        return False
    
    cows_val = row_values[col_map['total_cows']]
    cows_num = _parse_european_number(cows_val)
    
    # Heuristic: if total cows > 2000, it's likely a grand total
    # (typical daily count is 200-800 per day)
    if cows_num and cows_num > 2000:
        return True
    
    return False


def _parse_european_number(value: Any) -> Optional[float]:
    """
    Parse a number that may use European formatting.
    
    European: 1.234,56 (thousands=dot, decimal=comma)
    Standard: 1,234.56 (thousands=comma, decimal=dot)
    
    Delaval uses comma as thousands separator in totals: "1,703"
    """
    if value is None:
        return None
    
    str_val = str(value).strip()
    if not str_val:
        return None
    
    # Strip SUM= prefix if present (e.g., "SUM=39.8" -> "39.8")
    if str_val.upper().startswith('SUM='):
        str_val = str_val[4:].strip()
    
    try:
        # First try direct parse
        return float(str_val)
    except ValueError:
        pass
    
    # Handle comma as thousands separator: "1,703" -> "1703"
    # But also handle comma as decimal: "13,5" -> "13.5"
    if ',' in str_val and '.' not in str_val:
        # Check if it looks like thousands (digits after comma are 3)
        parts = str_val.split(',')
        if len(parts) == 2 and len(parts[1]) == 3 and parts[1].isdigit():
            # Thousands separator: "1,703" -> "1703"
            str_val = str_val.replace(',', '')
        else:
            # Decimal separator: "13,5" -> "13.5"
            str_val = str_val.replace(',', '.')
    
    try:
        return float(str_val)
    except ValueError:
        return None


def _extract_daily_record(
    row_values: List[Any],
    col_map: Dict[str, int],
    date_str: str,
    parlour_name: str,
    session_yields: List[float],
    session_cows: List[float]
) -> Optional[Dict[str, Any]]:
    """
    Extract a daily record from a totals row.
    
    The daily yield per cow is computed as the sum of session yields.
    If the totals row has a SUM= prefixed value, use that directly.
    Otherwise, sum the accumulated session yields.
    
    Returns dict with columns matching dairy_production mapping:
    - Date: production date
    - Day production/cow (kg): milk yield per cow (sum of sessions)
    - Nr. animals: average cows per session
    """
    try:
        # Get milk yield per cow from totals row
        yield_per_cow = None
        if 'milk_yield_per_cow' in col_map:
            raw_yield = row_values[col_map['milk_yield_per_cow']]
            raw_str = str(raw_yield).strip() if raw_yield else ''
            
            # If totals row has SUM= prefix, it's already the daily sum
            if raw_str.upper().startswith('SUM='):
                yield_per_cow = _parse_european_number(raw_yield)
            elif session_yields:
                # Sum the accumulated session yields
                yield_per_cow = sum(session_yields)
            else:
                # Fallback to totals row value (may be average)
                yield_per_cow = _parse_european_number(raw_yield)
        
        # Calculate number of animals as average across sessions
        num_animals = None
        if session_cows:
            num_animals = int(sum(session_cows) / len(session_cows))
        elif 'total_cows' in col_map:
            # Fallback to totals row value
            total_cows = _parse_european_number(row_values[col_map['total_cows']])
            if total_cows:
                num_animals = int(total_cows)
        
        # Validate we have minimum data
        if num_animals is None and yield_per_cow is None:
            logger.debug("Skipping row with no valid data for %s", date_str)
            return None
        
        # Build record with column names matching the mapping file
        record = {
            'Date': date_str,
            'Day production/cow (kg)': yield_per_cow,
            'Nr. animals': num_animals,
            # Additional metadata
            'parlour_name': parlour_name,
        }
        
        # Get total milk yield if available
        if 'milk_yield' in col_map:
            record['total_milk_yield_kg'] = _parse_european_number(
                row_values[col_map['milk_yield']]
            )
        
        # Get cows identified if available
        if 'cows_identified' in col_map:
            record['cows_identified'] = _parse_european_number(
                row_values[col_map['cows_identified']]
            )

        return record
        
    except Exception as e:
        logger.warning("Failed to extract daily record for %s: %s", date_str, e)
        return None


def is_delaval_format(columns: List[str]) -> bool:
    """
    Check if a file appears to be in Delaval format.
    
    Args:
        columns: List of column names from the file
        
    Returns:
        True if file matches Delaval format signatures
    """
    if not columns:
        return False
    
    cols_lower = {str(c).lower().strip() for c in columns}
    
    # Key Delaval columns
    delaval_markers = {
        'session number',
        'milk start time', 
        'total cows',
        'milk yield',
        'milk yield per cow',
    }
    
    # Check for significant overlap
    matches = cols_lower & delaval_markers
    if len(matches) >= 3:
        return True
    
    # Also check for the specific "Milk Yield per Cow" without exact match
    for col in cols_lower:
        if 'milk yield' in col and 'cow' in col:
            if 'session number' in cols_lower or 'total cows' in cols_lower:
                return True
    
    return False
