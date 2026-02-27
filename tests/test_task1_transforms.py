"""Unit tests for Task 1 transformation functions."""
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from task1_ingestion import (  # pylint: disable=wrong-import-position
    transform_integer,
    transform_decimal,
    transform_timestamp,
    transform_boolean,
    transform_holiday,
)


# =============================================================================
# TEST transform_integer
# =============================================================================

def test_integer_normal():
    """Normal integer value."""
    assert transform_integer("55") == 55

def test_integer_with_comma():
    """Integer with comma should be valid."""
    assert transform_integer("261, 18") == 26118

def test_integer_dash_is_null():
    """Dash is invalid → None."""
    assert transform_integer("-") is None

def test_integer_text_is_null():
    """Text is invalid → None."""
    assert transform_integer("abc") is None

def test_integer_empty_is_null():
    """Empty string → None."""
    assert transform_integer("") is None

def test_integer_float_string():
    """Float string should convert to int."""
    assert transform_integer("55.0") == 55


# =============================================================================
# TEST transform_decimal
# =============================================================================

def test_decimal_normal():
    """Normal decimal value."""
    assert transform_decimal("456.789") == 456.789

def test_decimal_large_number():
    """Large number beyond NUMERIC precision → float."""
    result = transform_decimal("519975958309744380574202244404.0")
    assert isinstance(result, float)

def test_decimal_dash_is_null():
    """Dash is invalid → None."""
    assert transform_decimal("-") is None

def test_decimal_hash_is_null():
    """Hash symbol is invalid → None."""
    assert transform_decimal("#") is None

def test_decimal_scientific_notation():
    """Scientific notation should be valid."""
    result = transform_decimal("1.5e10")
    assert result == 1.5e10


# =============================================================================
# TEST transform_timestamp
# =============================================================================

def test_timestamp_standard_format():
    """Standard datetime format."""
    result = transform_timestamp("2026-01-06 07:48:25")
    assert result == datetime(2026, 1, 6, 7, 48, 25)

def test_timestamp_14digit():
    """14-digit compact format YYYYMMDDHHmmss."""
    result = transform_timestamp("20260106074825")
    assert result == datetime(2026, 1, 6, 7, 48, 25)

def test_timestamp_ddmmyyyy():
    """DD/MM/YYYY format → time defaults to 00:00:00."""
    result = transform_timestamp("06/01/2026")
    assert result == datetime(2026, 1, 6, 0, 0, 0)

def test_timestamp_dd_mon_yy():
    """DD-Mon-YY format."""
    result = transform_timestamp("06-Jan-26")
    assert result == datetime(2026, 1, 6, 0, 0, 0)

def test_timestamp_random_is_null():
    """'random format' text → None."""
    assert transform_timestamp("random format") is None

def test_timestamp_empty_is_null():
    """Empty string → None."""
    assert transform_timestamp("") is None


# =============================================================================
# TEST transform_boolean
# =============================================================================

def test_boolean_true_variants():
    """All true variants."""
    for val in ('true', 'True', 'TRUE', 'yes', 'Yes', 'ok', 'Ok', '1'):
        assert transform_boolean(val) is True, f"Expected True for '{val}'"

def test_boolean_false_variants():
    """False variants."""
    for val in ('false', 'no', '0', 'other'):
        assert transform_boolean(val) is False, f"Expected False for '{val}'"

def test_boolean_dash_is_null():
    """Dash is invalid → None."""
    assert transform_boolean("-") is None

def test_boolean_empty_is_null():
    """Empty string → None."""
    assert transform_boolean("") is None


# =============================================================================
# TEST transform_holiday
# =============================================================================

def test_holiday_normal():
    """Normal holiday name."""
    assert transform_holiday("Makha Bucha") == "Makha Bucha"

def test_holiday_from_sentence():
    """Extract holiday name from sentence."""
    result = transform_holiday("Today is Songkran Festival, offices are closed")
    assert result == "Songkran Festival"

def test_holiday_dash_is_null():
    """Dash → None."""
    assert transform_holiday("-") is None

def test_holiday_empty_is_null():
    """Empty string → None."""
    assert transform_holiday("") is None

def test_holiday_unknown_sentence_is_null():
    """Sentence with no known holiday → None."""
    result = transform_holiday("Today is a very special day for everyone")
    assert result is None

def test_holiday_whitespace_stripped():
    """Whitespace should be stripped."""
    assert transform_holiday("  Makha Bucha  ") == "Makha Bucha"
