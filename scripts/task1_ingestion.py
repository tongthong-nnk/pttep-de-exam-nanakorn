import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import pytz
import re
import logging
import sys

# =============================================================================
# LOGGING SETUP
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('../logs/task1_ingestion.log'),
    ]
)
logger = logging.getLogger(__name__)

# =============================================================================
# GLOBAL CONFIGURATION
# =============================================================================
PROJECT_ID  = "pttep-exam-tongthong"
DATASET_ID  = "exam_nanakorn"
TABLE_ID    = "task1_data_result"
INPUT_FILE  = "../data/de-exam-task1_data_storytelling.csv"

# =============================================================================
# TRANSFORMATION FUNCTIONS
# =============================================================================

def transform_integer(val):
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
    if pd.isna(val):
        return None
    val = str(val).strip().lower()
    if val in ('', 'nan', '-'):
        return None
    if val in ('true', 'yes', 'ok', '1'):
        return True
    return False

KNOWN_HOLIDAYS = [
    'Makha Bucha', 'Labour Day', 'Songkran Festival',
    'King Bhumibol Memorial Day', "King's Birthday",
    'Chulalongkorn Day', 'Royal Ploughing Ceremony',
    'Constitution Day', 'New Year', 'Christmas',
    'Coronation Day', 'Queen Suthida Birthday',
]

def transform_holiday(val):
    if pd.isna(val):
        return None
    val = str(val).strip()
    if val in ('', '-', 'nan'):
        return None
    # ถ้าสั้นพอ (ไม่ใช่ประโยค) return ตรงๆ
    if len(val.split()) <= 4:
        return val
    # ถ้าเป็นประโยค ให้ extract holiday name ออกมา
    val_lower = val.lower()
    for holiday in KNOWN_HOLIDAYS:
        if holiday.lower() in val_lower:
            return holiday
    return None

# =============================================================================
# DATA VALIDATION
# =============================================================================

def validate_dataframe(df):
    logger.info("--- Data Quality Report ---")
    passed = True

    # Check row count
    logger.info(f"  Total rows       : {len(df)}")

    # Check each column null %
    for col in ['integer_col', 'decimal_col', 'timestamp_col', 'boolean_col', 'holiday_name']:
        nulls = df[col].isna().sum()
        pct   = nulls / len(df) * 100
        logger.info(f"  {col:<20}: {nulls:>3} nulls ({pct:.1f}%)")

    # Validate boolean only has True/False/None
    invalid_bool = df['boolean_col'].dropna().apply(
        lambda x: x not in (True, False)
    ).sum()
    if invalid_bool > 0:
        logger.error(f"  boolean_col has {invalid_bool} invalid values!")
        passed = False
    else:
        logger.info(f"  boolean_col values: True={df['boolean_col'].eq(True).sum()}, False={df['boolean_col'].eq(False).sum()}, NULL={df['boolean_col'].isna().sum()}")

    # Validate row_id is unique
    if df['row_id'].nunique() != len(df):
        logger.error("  row_id is NOT unique!")
        passed = False
    else:
        logger.info(f"  row_id           : unique ✓")

    # Validate audit columns exist
    for col in ['business_datetime', 'created_datetime']:
        if col not in df.columns:
            logger.error(f"  Missing column: {col}")
            passed = False
        else:
            logger.info(f"  {col:<20}: present ✓")

    if passed:
        logger.info("  Validation PASSED ✓")
    else:
        logger.error("  Validation FAILED ✗")

    return passed

# =============================================================================
# MAIN INGESTION
# =============================================================================

def run_ingestion():
    client = bigquery.Client(project=PROJECT_ID)
    destination_table = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    logger.info(f"Starting Task 1 ingestion: {INPUT_FILE}")

    try:
        df = pd.read_csv(INPUT_FILE, dtype=str)
        df.columns = df.columns.str.strip().str.lower()
        logger.info(f"Loaded {len(df)} rows. Columns: {list(df.columns)}")
    except Exception as e:
        logger.error(f"Error reading CSV: {e}")
        return

    # row_id
    df.insert(0, 'row_id', range(1, len(df) + 1))

    # Transformations
    logger.info("Applying transformations...")
    df['integer_col']   = df['integer_col'].apply(transform_integer)
    df['decimal_col']   = df['decimal_col'].apply(transform_decimal)
    df['timestamp_col'] = df['timestamp_col'].apply(transform_timestamp)
    df['boolean_col']   = df['boolean_col'].apply(transform_boolean)
    df['holiday_name']  = df['holiday_name'].apply(transform_holiday)

    # Audit columns
    bkk_tz = pytz.timezone('Asia/Bangkok')
    utc_tz  = pytz.utc
    now_utc = datetime.now(utc_tz)
    df['business_datetime'] = now_utc.astimezone(bkk_tz)
    df['created_datetime']  = now_utc

    # Cast types
    df['integer_col'] = pd.array(df['integer_col'], dtype=pd.Int64Dtype())
    df['decimal_col'] = pd.to_numeric(df['decimal_col'], errors='coerce')

    logger.info("Transformations complete.")

    # Validate
    if not validate_dataframe(df):
        logger.error("Aborting due to validation failure.")
        return

    # Load to BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("row_id",           "INTEGER",   mode="NULLABLE"),
            bigquery.SchemaField("integer_col",      "INTEGER",   mode="NULLABLE"),
            bigquery.SchemaField("decimal_col",      "FLOAT64",   mode="NULLABLE"),
            bigquery.SchemaField("timestamp_col",    "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("boolean_col",      "BOOL",      mode="NULLABLE"),
            bigquery.SchemaField("holiday_name",     "STRING",    mode="NULLABLE"),
            bigquery.SchemaField("business_datetime","TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("created_datetime", "TIMESTAMP", mode="NULLABLE"),
        ]
    )

    logger.info(f"Loading into BigQuery: {destination_table}")
    try:
        load_job = client.load_table_from_dataframe(df, destination_table, job_config=job_config)
        load_job.result()
        logger.info(f"SUCCESS: {len(df)} rows loaded into {destination_table}")
    except Exception as e:
        logger.error(f"BigQuery Load Error: {e}")

if __name__ == "__main__":
    run_ingestion()
