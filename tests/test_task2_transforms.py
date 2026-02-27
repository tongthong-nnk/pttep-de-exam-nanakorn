"""Unit tests for Task 2 transformation functions."""
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from task2_ingestion import (  # pylint: disable=wrong-import-position
    extract_parameter_from_filename,
    build_month_groups,
)


# =============================================================================
# TEST extract_parameter_from_filename
# =============================================================================

def test_parameter_standard_filename():
    """Standard filename with 8-digit date suffix."""
    result = extract_parameter_from_filename("DE_Exam_raw_data_20250101.xlsx")
    assert result == date(2025, 1, 1)

def test_parameter_different_date():
    """Different date in filename."""
    result = extract_parameter_from_filename("DE_Exam_raw_data_20251231.xlsx")
    assert result == date(2025, 12, 31)

def test_parameter_with_full_path():
    """Filename with full directory path."""
    result = extract_parameter_from_filename("/data/DE_Exam_raw_data_20250601.xlsx")
    assert result == date(2025, 6, 1)

def test_parameter_no_date_returns_none():
    """Filename without 8-digit date returns None."""
    result = extract_parameter_from_filename("DE_Exam_raw_data.xlsx")
    assert result is None


# =============================================================================
# TEST build_month_groups
# =============================================================================

def test_month_groups_count():
    """Should return exactly 4 month groups."""
    result = build_month_groups(date(2025, 1, 1))
    assert len(result) == 4

def test_month_groups_col_starts():
    """Column start indices should be 1, 7, 13, 19."""
    result = build_month_groups(date(2025, 1, 1))
    col_starts = [col_start for col_start, _ in result]
    assert col_starts == [1, 7, 13, 19]

def test_month_groups_dates():
    """Month dates should increment correctly from base date."""
    result = build_month_groups(date(2025, 1, 1))
    dates = [group_date for _, group_date in result]
    assert dates == [
        date(2025, 1, 1),
        date(2025, 2, 1),
        date(2025, 3, 1),
        date(2025, 4, 1),
    ]

def test_month_groups_year_rollover():
    """Month groups should handle year rollover correctly."""
    result = build_month_groups(date(2025, 11, 1))
    dates = [group_date for _, group_date in result]
    assert dates == [
        date(2025, 11, 1),
        date(2025, 12, 1),
        date(2026, 1, 1),
        date(2026, 2, 1),
    ]
