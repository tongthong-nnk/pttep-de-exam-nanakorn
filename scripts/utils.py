"""Shared utilities for PTTEP data ingestion pipeline."""
import logging
import os
import sys

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))


def get_config(key, default=None):
    """Get configuration value from environment variable."""
    return os.getenv(key, default)


def setup_logging(log_file):
    """Configure logging to stdout and file. Return logger instance."""
    os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file),
        ]
    )
    return logging.getLogger(__name__)


def load_to_bigquery(client, df, destination_table, schema):
    """Load DataFrame to BigQuery with WRITE_TRUNCATE disposition."""
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=schema
    )
    load_job = client.load_table_from_dataframe(
        df, destination_table, job_config=job_config
    )
    load_job.result()
    return load_job


def profile_dataframe(df, logger):
    """Generate and log a data profiling report for the dataframe."""
    logger.info("--- Data Profiling Report ---")
    logger.info("  Shape            : %d rows x %d cols", df.shape[0], df.shape[1])

    for col in df.columns:
        null_count = df[col].isna().sum()
        null_pct   = null_count / len(df) * 100
        unique     = df[col].nunique()

        if df[col].dtype in ('int64', 'float64', 'Int64') and col not in ('row_id',):
            col_clean = df[col].dropna()
            if len(col_clean) > 0:
                logger.info(
                    "  %-22s: nulls=%d(%.1f%%) unique=%d min=%.2f max=%.2f mean=%.2f",
                    col, null_count, null_pct, unique,
                    col_clean.min(), col_clean.max(), col_clean.mean()
                )
        elif df[col].dtype == 'object':
            top = df[col].value_counts().head(3).to_dict()
            logger.info(
                "  %-22s: nulls=%d(%.1f%%) unique=%d top=%s",
                col, null_count, null_pct, unique, top
            )
        elif df[col].dtype == 'bool' or col == 'boolean_col':
            logger.info(
                "  %-22s: nulls=%d(%.1f%%) unique=%d",
                col, null_count, null_pct, unique
            )
        else:
            logger.info(
                "  %-22s: nulls=%d(%.1f%%) unique=%d",
                col, null_count, null_pct, unique
            )
