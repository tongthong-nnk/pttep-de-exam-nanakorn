import openpyxl
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, date
import pytz
import re
import os

from utils import setup_logging, load_to_bigquery

# =============================================================================
# LOGGING SETUP
# =============================================================================
logger = setup_logging('../logs/task2_ingestion.log')

# =============================================================================
# GLOBAL CONFIGURATION
# =============================================================================
PROJECT_ID  = "pttep-exam-tongthong"
DATASET_ID  = "exam_nanakorn"
TABLE_ID    = "task2_data_result"
INPUT_FILE  = "../data/DE_Exam_raw_data_20250101.xlsx"

EXPECTED_ASSETS = {'Prod_Well_A', 'Pressure_Tx', 'Gas_Flow', 'Temp_Sensor_1', 'Inject_Pump_B'}

# =============================================================================
# HELPERS
# =============================================================================

def extract_parameter_from_filename(filepath):
    basename = os.path.basename(filepath)
    match = re.search(r'(\d{8})', basename)
    if match:
        return datetime.strptime(match.group(1), '%Y%m%d').date()
    return None

# =============================================================================
# DATA VALIDATION
# =============================================================================

def validate_dataframe(df):
    logger.info("--- Data Quality Report ---")
    passed = True

    logger.info(f"  Total rows       : {len(df)}")
    logger.info(f"  Date range       : {df['date'].min()} to {df['date'].max()}")
    logger.info(f"  Assets found     : {sorted(df['asset'].unique())}")
    logger.info(f"  Parameter        : {df['parameter'].unique()}")

    actual_assets = set(df['asset'].unique())
    missing = EXPECTED_ASSETS - actual_assets
    if missing:
        logger.error(f"  Missing assets: {missing}")
        passed = False
    else:
        logger.info(f"  All expected assets present ✓")

    for col in ['date', 'asset', 'nomination', 'load_ts']:
        nulls = df[col].isna().sum()
        if nulls > 0:
            logger.error(f"  {col} has {nulls} unexpected nulls!")
            passed = False
        else:
            logger.info(f"  {col:<20}: no nulls ✓")

    non_numeric = df['nomination'].apply(
        lambda x: not isinstance(x, (int, float))
    ).sum()
    if non_numeric > 0:
        logger.error(f"  nomination has {non_numeric} non-numeric values!")
        passed = False
    else:
        logger.info(f"  nomination       : all numeric ✓")

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

    logger.info(f"Starting Task 2 ingestion: {INPUT_FILE}")

    wb = openpyxl.load_workbook(INPUT_FILE)
    ws = wb.active
    all_rows = list(ws.iter_rows(values_only=True))

    base_date = all_rows[11][1]
    if isinstance(base_date, datetime):
        base_date = base_date.date()

    month_groups = []
    for i in range(4):
        col_start = 1 + (i * 6)
        month = base_date.month + i
        year  = base_date.year + (month - 1) // 12
        month = ((month - 1) % 12) + 1
        group_date = date(year, month, 1)
        month_groups.append((col_start, group_date))
        logger.info(f"  Group {i+1}: col_start={col_start}, month={group_date}")

    asset_offsets = [
        (1, 'Prod_Well_A'),
        (2, 'Pressure_Tx'),
        (3, 'Gas_Flow'),
        (4, 'Temp_Sensor_1'),
        (5, 'Inject_Pump_B'),
    ]

    parameter = extract_parameter_from_filename(INPUT_FILE)
    if parameter is None:
        parameter = base_date
        logger.warning(f"No date suffix in filename, using B12 date: {parameter}")
    else:
        logger.info(f"Parameter extracted from filename: {parameter}")

    utc_tz  = pytz.utc
    load_ts = datetime.now(utc_tz)

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

            for offset, asset_name in asset_offsets:
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

    df = pd.DataFrame(records)
    logger.info(f"Extracted {len(df)} rows")

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

    logger.info(f"Loading into BigQuery: {destination_table}")
    try:
        load_to_bigquery(client, df, destination_table, schema)
        logger.info(f"SUCCESS: {len(df)} rows loaded into {destination_table}")
    except Exception as e:
        logger.error(f"BigQuery Load Error: {e}")

if __name__ == "__main__":
    run_ingestion()
