from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from docker.types import Mount

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

    task1 = DockerOperator(
        task_id='task1_csv_to_bigquery',
        image='pttep-pipeline:latest',
        command='python scripts/task1_ingestion.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        auto_remove=True,
        environment={
            'GOOGLE_CLOUD_PROJECT': 'pttep-exam-tongthong',
            'PYTHONUNBUFFERED': '1',
        },
        mounts=[
            Mount(
                source='/home/tongthong_nnk/PTTEP_DE_EXAM/data',
                target='/app/data',
                type='bind',
            ),
            Mount(
                source='/home/tongthong_nnk/PTTEP_DE_EXAM/logs',
                target='/app/logs',
                type='bind',
            ),
            Mount(
                source='/home/tongthong_nnk/.config/gcloud',
                target='/root/.config/gcloud',
                type='bind',
            ),
        ],
        doc_md="""
        ## Task 1: CSV Ingestion (DockerOperator)
        - Image: pttep-pipeline:latest
        - Source: de-exam-task1_data_storytelling.csv
        - Destination: exam_nanakorn.task1_data_result
        - Transformations: boolean mapping, timestamp parsing, decimal handling, null handling
        """,
    )

    task2 = DockerOperator(
        task_id='task2_excel_to_bigquery',
        image='pttep-pipeline:latest',
        command='python scripts/task2_ingestion.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        auto_remove=True,
        environment={
            'GOOGLE_CLOUD_PROJECT': 'pttep-exam-tongthong',
            'PYTHONUNBUFFERED': '1',
        },
        mounts=[
            Mount(
                source='/home/tongthong_nnk/PTTEP_DE_EXAM/data',
                target='/app/data',
                type='bind',
            ),
            Mount(
                source='/home/tongthong_nnk/PTTEP_DE_EXAM/logs',
                target='/app/logs',
                type='bind',
            ),
            Mount(
                source='/home/tongthong_nnk/.config/gcloud',
                target='/root/.config/gcloud',
                type='bind',
            ),
        ],
        doc_md="""
        ## Task 2: Excel Ingestion (DockerOperator)
        - Image: pttep-pipeline:latest
        - Source: DE_Exam_raw_data_20250101.xlsx
        - Destination: exam_nanakorn.task2_data_result
        - Transformations: unpivot wide->long, extract parameter from filename, remove AVG rows
        """,
    )

    # Task 1 runs first, then Task 2
    task1 >> task2
