# PTTEP Data Engineering Exam
**Candidate:** Nanakorn Tongthong  
**Dataset:** exam_nanakorn  
**GCP Project:** pttep-exam-tongthong  
**Pylint Score:** 9.97/10

---

## Project Structure
```
PTTEP_DE_EXAM/
├── .github/
│   └── workflows/
│       └── pylint.yml           # CI/CD: Pylint + Pytest on every push
├── dags/
│   └── pttep_pipeline.py        # Airflow DAG with DockerOperator
├── data/
│   ├── de-exam-task1_data_storytelling.csv
│   └── DE_Exam_raw_data_20250101.xlsx
├── logs/
│   ├── task1_ingestion.log
│   └── task2_ingestion.log
├── scripts/
│   ├── task1_ingestion.py       # Task 1: CSV to BigQuery
│   ├── task2_ingestion.py       # Task 2: Excel to BigQuery
│   └── utils.py                 # Shared: logging, BigQuery loader, config, profiling
├── terraform/
│   └── main.tf                  # Infrastructure as Code (GCS + BigQuery)
├── tests/
│   └── test_task1_transforms.py # Unit tests for all transform functions
├── .env.example                 # Environment variable template
├── .pylintrc                    # Pylint configuration
├── Dockerfile                   # Container image for pipeline
├── docker-compose.yml           # Multi-service container setup
├── requirements.txt             # Production dependencies (pinned versions)
├── requirements-lint.txt        # CI/CD dependencies (unpinned for compatibility)
└── README.md
```

---

## Prerequisites
- Python 3.11+
- Google Cloud SDK (`gcloud`)
- Terraform
- Docker & Docker Compose (for containerized execution)

---

## Environment Setup

### 1. Clone Repository
```bash
git clone https://github.com/tongthong-nnk/pttep-de-exam-nanakorn.git
cd pttep-de-exam-nanakorn
```

### 2. Configure Environment Variables
```bash
cp .env.example .env
# Edit .env with your actual GCP project values
```

`.env` contains:
```
PROJECT_ID=your-gcp-project-id
DATASET_ID=exam_nanakorn
TABLE_ID_TASK1=task1_data_result
TABLE_ID_TASK2=task2_data_result
INPUT_FILE_TASK1=../data/de-exam-task1_data_storytelling.csv
INPUT_FILE_TASK2=../data/DE_Exam_raw_data_20250101.xlsx
```

### 3. Authenticate with GCP
```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project pttep-exam-tongthong
```

### 4. Provision Infrastructure with Terraform
```bash
cd terraform/
terraform init
terraform plan
terraform apply
```

Creates:
- GCS Bucket: `pttep-exam-data-nanakorn-2026`
- BigQuery Dataset: `exam_nanakorn`
- BigQuery Table: `task1_data_result`
- BigQuery Table: `task2_data_result`

---

## How to Run

### Option A: Python (Local)
```bash
pip install -r requirements.txt

cd scripts/
python3 task1_ingestion.py
python3 task2_ingestion.py
```

### Option B: Docker
```bash
# Build image
docker build -t pttep-pipeline .

# Run Task 1
docker-compose run task1

# Run Task 2
docker-compose run task2
```

### Option C: Airflow with DockerOperator
```bash
export AIRFLOW_HOME=~/airflow
airflow db init
cp dags/pttep_pipeline.py ~/airflow/dags/
airflow scheduler &
airflow webserver
```
Pipeline runs daily at 08:00 UTC: `task1_csv_to_bigquery >> task2_excel_to_bigquery`

---

## Running Tests
```bash
pip install pytest
python3 -m pytest tests/ -v
```

Covers 24 test cases across all transform functions:
- `transform_integer` — comma handling, invalid values
- `transform_decimal` — large numbers, scientific notation, invalid symbols
- `transform_timestamp` — 4 date formats, edge cases
- `transform_boolean` — all true/false variants
- `transform_holiday` — sentence extraction, whitespace, unknown values

---

## Pipeline Design

### Task 1: CSV Ingestion
**Source:** `de-exam-task1_data_storytelling.csv` (105 rows)  
**Destination:** `exam_nanakorn.task1_data_result`

| Column | Type | Transformation |
|---|---|---|
| `row_id` | INTEGER | Auto-generated 1 to N as audit key |
| `integer_col` | INTEGER | Remove commas: `"261, 18"` to `26118`, invalid to NULL |
| `decimal_col` | FLOAT64 | Values exceed NUMERIC range (42+ digits), `#` and `-` to NULL |
| `timestamp_col` | TIMESTAMP | Parse 4 formats: `YYYY-MM-DD HH:MM:SS`, `DD/MM/YYYY`, `DD-Mon-YY`, `YYYYMMDDHHmmss` |
| `boolean_col` | BOOL | `true/yes/ok/1` to True, `-` to NULL, others to False |
| `holiday_name` | STRING | Extract from sentence if needed, `-` and empty to NULL |
| `business_datetime` | TIMESTAMP | Ingestion time in Asia/Bangkok timezone |
| `created_datetime` | TIMESTAMP | Ingestion time in UTC timezone |

### Task 2: Excel Ingestion
**Source:** `DE_Exam_raw_data_20250101.xlsx`  
**Destination:** `exam_nanakorn.task2_data_result` (600 rows)

| Step | Logic |
|---|---|
| Skip rows 1-13 | Metadata, notes, and column definitions |
| Unpivot wide to long | 4 month groups x 5 assets x ~30 days = 600 rows |
| Remove AVG row | Row 45 contains AVERAGE formulas, excluded |
| Remove Reference column | Engineer remarks omitted per requirements |
| Extract parameter | Filename suffix `20250101` to `2025-01-01` |
| load_ts | Current UTC timestamp added on ingestion |

---

## Key Design Decisions

**FLOAT64 over NUMERIC for decimal_col**  
Source data contains values up to 42 digits which exceeds BigQuery NUMERIC precision (29 digits). FLOAT64 handles these values correctly.

**Idempotent Loading with WRITE_TRUNCATE**  
Both pipelines use `WRITE_TRUNCATE` ensuring re-runs produce consistent results without duplicates. Safe to run multiple times.

**Validation Gates before Load**  
Each script validates data quality after transformation and aborts if validation fails, preventing corrupt data from reaching BigQuery.

**Shared Utils Module**  
Common functions (logging setup, BigQuery loader, config reader, data profiler) extracted to `utils.py` following the DRY principle. Pylint score: 9.97/10.

**Environment Variables via .env**  
All configuration loaded from `.env` file. Allows deployment to different environments without code changes.

**DockerOperator in Airflow**  
Each task runs in its own container via DockerOperator, providing isolation, reproducibility, and Kubernetes-style execution.

**Terraform for Infrastructure**  
All GCP resources defined as code ensuring reproducible environment setup across teams.

---

## CI/CD Pipeline

Every push to `main` triggers GitHub Actions (2 parallel workflows):
```
Workflow 1 (Pylint & Pytest):
push -> Install dependencies -> Pylint (9.97/10) -> Pytest (24 tests) -> pass

Workflow 2 (Security Scan):
push -> Bandit security scan (Medium+ severity) -> pass
```

Runs on Python 3.11 and 3.12.

---

## Sample Logs

### Task 1
```
2026-02-27 07:57:59 [INFO] Starting Task 1 ingestion: ../data/de-exam-task1_data_storytelling.csv
2026-02-27 07:57:59 [INFO] Loaded 105 rows. Columns: ['integer_col', 'decimal_col', 'timestamp_col', 'boolean_col', 'holiday_name']
2026-02-27 07:57:59 [INFO] Applying transformations...
2026-02-27 07:57:59 [INFO] Transformations complete.
2026-02-27 07:57:59 [INFO] --- Data Profiling Report ---
2026-02-27 07:57:59 [INFO]   Shape            : 105 rows x 8 cols
2026-02-27 07:57:59 [INFO]   integer_col      : nulls=46(43.8%) unique=48 min=1.00 max=55216.00 mean=3403.83
2026-02-27 07:57:59 [INFO]   decimal_col      : nulls=32(30.5%) unique=54 min=456.79 max=1234567890123456730939583435068208897327104.00
2026-02-27 07:57:59 [INFO]   boolean_col      : nulls=16(15.2%) unique=2 top={True: 48, False: 41}
2026-02-27 07:57:59 [INFO]   holiday_name     : nulls=23(21.9%) unique=12
2026-02-27 07:57:59 [INFO] --- Data Quality Report ---
2026-02-27 07:57:59 [INFO]   Total rows       : 105
2026-02-27 07:57:59 [INFO]   boolean_col values: True=48, False=41, NULL=16
2026-02-27 07:57:59 [INFO]   row_id           : unique
2026-02-27 07:57:59 [INFO]   Validation PASSED
2026-02-27 07:58:02 [INFO] SUCCESS: 105 rows loaded into pttep-exam-tongthong.exam_nanakorn.task1_data_result
```

### Task 2
```
2026-02-27 07:58:04 [INFO] Starting Task 2 ingestion: ../data/DE_Exam_raw_data_20250101.xlsx
2026-02-27 07:58:04 [INFO]   Group 1: col_start=1, month=2025-01-01
2026-02-27 07:58:04 [INFO]   Group 2: col_start=7, month=2025-02-01
2026-02-27 07:58:04 [INFO]   Group 3: col_start=13, month=2025-03-01
2026-02-27 07:58:04 [INFO]   Group 4: col_start=19, month=2025-04-01
2026-02-27 07:58:04 [INFO] Parameter extracted from filename: 2025-01-01
2026-02-27 07:58:04 [INFO] Extracted 600 rows
2026-02-27 07:58:04 [INFO] --- Data Profiling Report ---
2026-02-27 07:58:04 [INFO]   Shape            : 600 rows x 5 cols
2026-02-27 07:58:04 [INFO]   nomination       : nulls=0(0.0%) unique=527 min=0.00 max=999.00 mean=349.45
2026-02-27 07:58:04 [INFO] --- Data Quality Report ---
2026-02-27 07:58:04 [INFO]   All expected assets present
2026-02-27 07:58:04 [INFO]   Validation PASSED
2026-02-27 07:58:09 [INFO] SUCCESS: 600 rows loaded into pttep-exam-tongthong.exam_nanakorn.task2_data_result
```

---

## Production Considerations

If deploying this pipeline in a real production environment, the following improvements would be made:

**Infrastructure and Security**
- Use **Google Secret Manager** instead of `.env` for storing credentials and sensitive configuration
- Use **Workload Identity Federation** instead of Service Account key files
- Store Terraform state in a **GCS backend** to enable team collaboration
- Enable **BigQuery column-level security** for sensitive data fields

**Pipeline Reliability**
- Add a **Dead Letter Queue** for rows that fail validation instead of aborting the entire batch
- Use **BigQuery table partitioning** on `created_datetime` for improved query performance
- Add **data lineage tracking** using OpenLineage or Google Dataplex
- Switch to **incremental loading** instead of WRITE_TRUNCATE for large-scale datasets

**Observability**
- Send pipeline metrics to **Cloud Monitoring** such as row count, null rate, and pipeline duration
- Configure **alerting** when validation fails or pipeline duration exceeds SLA
- Define **data quality SLAs** such as maximum acceptable null rate per column

**Scalability**
- Use **Dataflow** instead of pandas for GB+ scale data processing
- Use **Cloud Composer** (managed Airflow) instead of self-hosted
- Use **Kubernetes on GKE** instead of local Docker for container orchestration
