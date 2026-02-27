"""Shared utilities for PTTEP data ingestion pipeline."""
import logging
import sys

from google.cloud import bigquery


def setup_logging(log_file):
    """Configure logging to stdout and file. Return logger instance."""
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
