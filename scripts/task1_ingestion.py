"""
Task 1: CSV Ingestion Pipeline
Reads de-exam-task1_data_storytelling.csv, applies data transformations,
validates data quality, and loads into BigQuery table task1_data_result.
"""
import re
from datetime import datetime

import pandas as pd
import pytz
from google.cloud import bigquery

from utils import setup_logging, load_to_bigquery

# =============================================================================
# LOGGING SETUP
# =============================================================================
logger = setup_logging('../logs/task1_ingestion.log')

# =============================================================================
# GLOBAL CONFIGURATION
# =============================================================================
PROJECT_ID  = "pttep-exam-tongthong"
DATASET_ID  = "exam_nanakorn"
TABLE_ID    = "task1_data_result"
INPUT_FILE  = "../data/de-exam-task1_data_storytelling.csv"

# =============================================================================
# KNOWN HOLIDAYS
# =============================================================================
KNOWN_HOLIDAYS = [
    'Makha Bucha', 'Labour Day', 'Songkran Festival',
    'King Bhumibol Memorial Day', "King's Birthday",
    'Chulalongkorn Day', 'Royal Ploughing Ceremony',
    'Constitution Day', 'New Year', 'Christmas',
    'Coronation Day', 'Queen Suthida Birthday',
]

# =============================================================================
# TRANSFORMATION FUNCTIONS
# =============================================================================

def transform_integer(val):
    """Convert value to integer. Remove commas. Return None for invalid values."""
    if pd.isna(val):
        return None
    val = str(val).strip()
    if val in ('', '-', 'nan'):
        return None
    val = val.replace(',', '').replace(' ', '')
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None

def transform_decimal(val):
    """Convert value to float. Return None for invalid values like '-' or '#'."""
    if pd.isna(val):
        return None
    val = str(val).strip()
    if val in ('', '-', '#', 'nan'):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

def transform_timestamp(val):
    """Parse timestamp from multiple formats. Return None for unparseable values."""
    if pd.isna(val):
        return None
    val = str(val).strip()
    if val in ('', 'nan', 'random format'):
        return None
    if re.match(r'^\d{14}$', val):
        try:
            return datetime.strptime(val, '%Y%m%d%H%M%S')
        except ValueError:
            return None
    formats = [
        '%Y-%m-%d %H:%M:%S',
        '%d/%m/%Y',
        '%d-%b-%y',
        '%Y-%m-%d',
    ]
    for fmt in formats:
        try:
            return datetime.strptime(val, fmt)
        except ValueError:
            continue
    return None

def transform_boolean(val):
    """Map value to boolean. true/yes/ok/1=True, '-'=None, others=False."""
    if pd.isna(val):
        return None
    val = str(val).strip().lower()
    if val in ('', 'nan', '-'):
        return None
    if val in ('true', 'yes', 'ok', '1'):
        return True
    return False

def transform_holiday(val):
    """Extract holiday name from value or sentence. Return None for invalid values."""
    if pd.isna(val):
        return None
    val = str(val).strip()
    if val in ('', '-', 'nan'):
        return None
    if len(val.split()) <= 4:
        return val
    val_lower = val.lower()
    for holiday in KNOWN_HOLIDAYS:
        if holiday.lower() in val_lower:
            return holiday
    return None

# =============================================================================
# DATA VALIDATION
# =============================================================================

def validate_dataframe(df):
    """Validate transformed dataframe and log data quality report."""
    logger.info("--- Data Quality Report ---")
    passed = True

    logger.info("  Total rows       : %d", len(df))

    for col in ['integer_col', 'decimal_col', 'timestamp_col', 'boolean_col', 'holiday_name']:
        nulls = df[col].isna().sum()
        pct   = nulls / len(df) * 100
        logger.info("  %-20s: %3d nulls (%.1f%%)", col, nulls, pct)

    invalid_bool = df['boolean_col'].dropna().apply(
        lambda x: x not in (True, False)
    ).sum()
    if invalid_bool > 0:
        logger.error("  boolean_col has %d invalid values!", invalid_bool)
        passed = False
    else:
        logger.info(
            "  boolean_col values: True=%d, False=%d, NULL=%d",
            df['boolean_col'].eq(True).sum(),
            df['boolean_col'].eq(False).sum(),
            df['boolean_col'].isna().sum()
        )

    if df['row_id'].nunique() != len(df):
        logger.error("  row_id is NOT unique!")
        passed = False
    else:
        logger.info("  row_id           : unique ✓")

    for col in ['business_datetime', 'created_datetime']:
        if col not in df.columns:
            logger.error("  Missing column: %s", col)
            passed = False
        else:
            logger.info("  %-20s: present ✓", col)

    if passed:
        logger.info("  Validation PASSED ✓")
    else:
        logger.error("  Validation FAILED ✗")

    return passed

# =============================================================================
# MAIN INGESTION
# =============================================================================

def run_ingestion():
    """Main ingestion function: read CSV, transform, validate, load to BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)
    destination_table = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    logger.info("Starting Task 1 ingestion: %s", INPUT_FILE)

    try:
        df = pd.read_csv(INPUT_FILE, dtype=str)
        df.columns = df.columns.str.strip().str.lower()
        logger.info("Loaded %d rows. Columns: %s", len(df), list(df.columns))
    except OSError as e:
        logger.error("Error reading CSV: %s", e)
        return

    df.insert(0, 'row_id', range(1, len(df) + 1))

    logger.info("Applying transformations...")
    df['integer_col']   = df['integer_col'].apply(transform_integer)
    df['decimal_col']   = df['decimal_col'].apply(transform_decimal)
    df['timestamp_col'] = df['timestamp_col'].apply(transform_timestamp)
    df['boolean_col']   = df['boolean_col'].apply(transform_boolean)
    df['holiday_name']  = df['holiday_name'].apply(transform_holiday)

    bkk_tz = pytz.timezone('Asia/Bangkok')
    utc_tz  = pytz.utc
    now_utc = datetime.now(utc_tz)
    df['business_datetime'] = now_utc.astimezone(bkk_tz)
    df['created_datetime']  = now_utc

    df['integer_col'] = pd.array(df['integer_col'], dtype=pd.Int64Dtype())
    df['decimal_col'] = pd.to_numeric(df['decimal_col'], errors='coerce')

    logger.info("Transformations complete.")

    if not validate_dataframe(df):
        logger.error("Aborting due to validation failure.")
        return

    schema = [
        bigquery.SchemaField("row_id",           "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("integer_col",      "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("decimal_col",      "FLOAT64",   mode="NULLABLE"),
        bigquery.SchemaField("timestamp_col",    "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("boolean_col",      "BOOL",      mode="NULLABLE"),
        bigquery.SchemaField("holiday_name",     "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("business_datetime","TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("created_datetime", "TIMESTAMP", mode="NULLABLE"),
    ]

    logger.info("Loading into BigQuery: %s", destination_table)
    try:
        load_to_bigquery(client, df, destination_table, schema)
        logger.info("SUCCESS: %d rows loaded into %s", len(df), destination_table)
    except OSError as e:
        logger.error("BigQuery Load Error: %s", e)

if __name__ == "__main__":
    run_ingestion()
