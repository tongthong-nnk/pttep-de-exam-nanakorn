from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

# =============================================================================
# DEFAULT ARGS
# =============================================================================
default_args = {
    'owner': 'nanakorn',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# =============================================================================
# DAG DEFINITION
# =============================================================================
with DAG(
    dag_id='pttep_data_ingestion_pipeline',
    default_args=default_args,
    description='PTTEP Data Engineering Exam - Ingestion Pipeline for Task1 and Task2',
    schedule_interval='0 8 * * *',  # Run daily at 8:00 AM UTC
    start_date=days_ago(1),
    catchup=False,
    tags=['pttep', 'ingestion', 'bigquery'],
) as dag:

    def run_task1():
        from task1_ingestion import run_ingestion
        run_ingestion()

    def run_task2():
        from task2_ingestion import run_ingestion
        run_ingestion()

    task1 = PythonOperator(
        task_id='task1_csv_to_bigquery',
        python_callable=run_task1,
        doc_md="""
        ## Task 1: CSV Ingestion
        - Source: de-exam-task1_data_storytelling.csv
        - Destination: exam_nanakorn.task1_data_result
        - Transformations: boolean mapping, timestamp parsing, decimal handling, null handling
        """,
    )

    task2 = PythonOperator(
        task_id='task2_excel_to_bigquery',
        python_callable=run_task2,
        doc_md="""
        ## Task 2: Excel Ingestion
        - Source: DE_Exam_raw_data_20250101.xlsx
        - Destination: exam_nanakorn.task2_data_result
        - Transformations: unpivot wide->long, extract parameter from filename, remove AVG rows
        """,
    )

    # Task 1 runs first, then Task 2
    task1 >> task2
