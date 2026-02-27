"""
Task 2: Excel Ingestion Pipeline
Reads DE_Exam_raw_data_20250101.xlsx, reshapes wide format to long format,
validates data quality, and loads into BigQuery table task2_data_result.
"""
import os
import re
from datetime import datetime, date

import openpyxl
import pandas as pd
import pytz
from google.cloud import bigquery

from utils import setup_logging, load_to_bigquery, get_config

# =============================================================================
# LOGGING SETUP
# =============================================================================
logger = setup_logging('../logs/task2_ingestion.log')

# =============================================================================
# GLOBAL CONFIGURATION
# =============================================================================
PROJECT_ID  = get_config("PROJECT_ID", "pttep-exam-tongthong")
DATASET_ID  = get_config("DATASET_ID", "exam_nanakorn")
TABLE_ID    = get_config("TABLE_ID_TASK2", "task2_data_result")
INPUT_FILE  = get_config("INPUT_FILE_TASK2", "../data/DE_Exam_raw_data_20250101.xlsx")

EXPECTED_ASSETS = {'Prod_Well_A', 'Pressure_Tx', 'Gas_Flow', 'Temp_Sensor_1', 'Inject_Pump_B'}

ASSET_OFFSETS = [
    (1, 'Prod_Well_A'),
    (2, 'Pressure_Tx'),
    (3, 'Gas_Flow'),
    (4, 'Temp_Sensor_1'),
    (5, 'Inject_Pump_B'),
]

# =============================================================================
# HELPERS
# =============================================================================

def extract_parameter_from_filename(filepath):
    """Extract 8-digit date suffix from filename and return as date object."""
    basename = os.path.basename(filepath)
    match = re.search(r'(\d{8})', basename)
    if match:
        return datetime.strptime(match.group(1), '%Y%m%d').date()
    return None

def build_month_groups(base_date):
    """Build list of (col_start, group_date) for each of the 4 month groups."""
    month_groups = []
    for i in range(4):
        col_start = 1 + (i * 6)
        month = base_date.month + i
        year  = base_date.year + (month - 1) // 12
        month = ((month - 1) % 12) + 1
        group_date = date(year, month, 1)
        month_groups.append((col_start, group_date))
        logger.info("  Group %d: col_start=%d, month=%s", i + 1, col_start, group_date)
    return month_groups

def extract_records(all_rows, month_groups, parameter, load_ts):
    """Extract and unpivot records from Excel rows into list of dicts."""
    records = []
    data_rows = all_rows[13:44]

    for row in data_rows:
        day_num = row[0]
        if day_num is None or str(day_num).upper() == 'AVG':
            continue
        try:
            day_num = int(day_num)
        except (ValueError, TypeError):
            continue

        for col_start, group_date in month_groups:
            try:
                record_date = date(group_date.year, group_date.month, day_num)
            except ValueError:
                continue

            for offset, asset_name in ASSET_OFFSETS:
                col_idx = col_start + offset - 1
                nomination = row[col_idx] if col_idx < len(row) else None

                if nomination is None:
                    continue
                if isinstance(nomination, str) and nomination.startswith('='):
                    continue
                try:
                    nomination = float(nomination)
                except (ValueError, TypeError):
                    continue

                records.append({
                    'date':       record_date,
                    'parameter':  parameter,
                    'asset':      asset_name.strip(),
                    'nomination': nomination,
                    'load_ts':    load_ts,
                })
    return records

# =============================================================================
# DATA VALIDATION
# =============================================================================

def validate_dataframe(df):
    """Validate transformed dataframe and log data quality report."""
    logger.info("--- Data Quality Report ---")
    passed = True

    logger.info("  Total rows       : %d", len(df))
    logger.info("  Date range       : %s to %s", df['date'].min(), df['date'].max())
    logger.info("  Assets found     : %s", sorted(df['asset'].unique()))
    logger.info("  Parameter        : %s", df['parameter'].unique())

    actual_assets = set(df['asset'].unique())
    missing = EXPECTED_ASSETS - actual_assets
    if missing:
        logger.error("  Missing assets: %s", missing)
        passed = False
    else:
        logger.info("  All expected assets present ✓")

    for col in ['date', 'asset', 'nomination', 'load_ts']:
        nulls = df[col].isna().sum()
        if nulls > 0:
            logger.error("  %s has %d unexpected nulls!", col, nulls)
            passed = False
        else:
            logger.info("  %-20s: no nulls ✓", col)

    non_numeric = df['nomination'].apply(
        lambda x: not isinstance(x, (int, float))
    ).sum()
    if non_numeric > 0:
        logger.error("  nomination has %d non-numeric values!", non_numeric)
        passed = False
    else:
        logger.info("  nomination       : all numeric ✓")

    if passed:
        logger.info("  Validation PASSED ✓")
    else:
        logger.error("  Validation FAILED ✗")

    return passed

# =============================================================================
# MAIN INGESTION
# =============================================================================

def run_ingestion():
    """Main ingestion function: read Excel, transform, validate, load to BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)
    destination_table = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    logger.info("Starting Task 2 ingestion: %s", INPUT_FILE)

    wb = openpyxl.load_workbook(INPUT_FILE)
    ws = wb.active
    all_rows = list(ws.iter_rows(values_only=True))

    base_date = all_rows[11][1]
    if isinstance(base_date, datetime):
        base_date = base_date.date()

    month_groups = build_month_groups(base_date)

    parameter = extract_parameter_from_filename(INPUT_FILE)
    if parameter is None:
        parameter = base_date
        logger.warning("No date suffix in filename, using B12 date: %s", parameter)
    else:
        logger.info("Parameter extracted from filename: %s", parameter)

    load_ts = datetime.now(pytz.utc)
    records = extract_records(all_rows, month_groups, parameter, load_ts)

    df = pd.DataFrame(records)
    logger.info("Extracted %d rows", len(df))
    from utils import profile_dataframe
    profile_dataframe(df, logger)

    if not validate_dataframe(df):
        logger.error("Aborting due to validation failure.")
        return

    schema = [
        bigquery.SchemaField("date",       "DATE",      mode="NULLABLE"),
        bigquery.SchemaField("parameter",  "DATE",      mode="NULLABLE"),
        bigquery.SchemaField("asset",      "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("nomination", "FLOAT64",   mode="NULLABLE"),
        bigquery.SchemaField("load_ts",    "TIMESTAMP", mode="NULLABLE"),
    ]

    logger.info("Loading into BigQuery: %s", destination_table)
    try:
        load_to_bigquery(client, df, destination_table, schema)
        logger.info("SUCCESS: %d rows loaded into %s", len(df), destination_table)
    except OSError as e:
        logger.error("BigQuery Load Error: %s", e)

if __name__ == "__main__":
    run_ingestion()
