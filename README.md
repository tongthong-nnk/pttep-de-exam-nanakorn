# PTTEP Data Engineering Exam
**Candidate:** Nanakorn  
**Dataset:** exam_nanakorn  
**Project:** pttep-exam-tongthong

---

## Project Structure
```
PTTEP_DE_EXAM/
├── dags/
│   └── pttep_pipeline.py        # Airflow DAG for orchestration
├── data/
│   ├── de-exam-task1_data_storytelling.csv
│   └── DE_Exam_raw_data_20250101.xlsx
├── logs/
│   ├── task1_ingestion.log
│   └── task2_ingestion.log
├── scripts/
│   ├── task1_ingestion.py       # Task 1: CSV -> BigQuery
│   └── task2_ingestion.py       # Task 2: Excel -> BigQuery
├── terraform/
│   └── main.tf                  # Infrastructure as Code
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

---

## Prerequisites
- Python 3.12+
- Google Cloud SDK (gcloud)
- Terraform
- Docker & Docker Compose (optional)

---

## GCP Setup

### 1. Authenticate
```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project pttep-exam-tongthong
```

### 2. Provision Infrastructure with Terraform
```bash
cd terraform/
terraform init
terraform apply
```

This creates:
- GCS Bucket: `pttep-exam-data-nanakorn-2026`
- BigQuery Dataset: `exam_nanakorn`
- BigQuery Table: `task1_data_result`
- BigQuery Table: `task2_data_result`

---

## How to Run

### Option A: Run directly with Python
```bash
pip install -r requirements.txt

# Task 1
python scripts/task1_ingestion.py

# Task 2
python scripts/task2_ingestion.py
```

### Option B: Run with Docker
```bash
# Build image
docker build -t pttep-pipeline .

# Run Task 1
docker-compose run task1

# Run Task 2
docker-compose run task2
```

### Option C: Run with Airflow
```bash
# Set Airflow home
export AIRFLOW_HOME=~/airflow

# Initialize Airflow
airflow db init

# Copy DAG
cp dags/pttep_pipeline.py ~/airflow/dags/

# Start Airflow
airflow scheduler &
airflow webserver
```
Pipeline runs daily at 08:00 UTC: `task1_csv_to_bigquery >> task2_excel_to_bigquery`

---

## Pipeline Design

### Task 1: CSV Ingestion
**Source:** `de-exam-task1_data_storytelling.csv`  
**Destination:** `exam_nanakorn.task1_data_result`

**Transformations:**
| Column | Logic |
|---|---|
| `integer_col` | Remove commas (e.g. `"261, 18"` → `26118`), invalid → NULL |
| `decimal_col` | FLOAT64 (values exceed NUMERIC range), `#`/`-` → NULL |
| `timestamp_col` | Parse 4 formats: `YYYY-MM-DD HH:MM:SS`, `DD/MM/YYYY`, `DD-Mon-YY`, `YYYYMMDDHHmmss` |
| `boolean_col` | `true/yes/ok/1` → True, `-` → NULL, others → False |
| `holiday_name` | Strip whitespace, `-`/empty → NULL |
| `row_id` | Auto-generated primary key for row-level tracking |
| `business_datetime` | Ingestion time in Asia/Bangkok timezone |
| `created_datetime` | Ingestion time in UTC timezone |

### Task 2: Excel Ingestion
**Source:** `DE_Exam_raw_data_20250101.xlsx`  
**Destination:** `exam_nanakorn.task2_data_result`

**Transformations:**
| Step | Logic |
|---|---|
| Skip header rows | Rows 1-13 contain metadata and column definitions |
| Unpivot wide → long | 4 month groups × 5 assets = 20 column groups → long format |
| Remove AVG rows | Row 45 contains AVERAGE formulas → excluded |
| Remove Reference column | Engineer remarks column omitted per requirements |
| Extract parameter | Date suffix from filename `20250101` → `2025-01-01` |
| load_ts | Current UTC timestamp added on ingestion |

---

## Key Design Decisions

**FLOAT64 over NUMERIC for decimal_col**  
Source data contains values up to 42 digits which exceeds BigQuery NUMERIC precision (29 digits). FLOAT64 handles these values correctly.

**Idempotent Loading (WRITE_TRUNCATE)**  
Both pipelines use `WRITE_TRUNCATE` to ensure re-runs produce consistent results without duplicates.

**Data Validation before Load**  
Each script runs a quality check after transformation and aborts if validation fails, preventing corrupt data from reaching BigQuery.

**Terraform for Infrastructure**  
All GCP resources are defined as code ensuring reproducible environment setup.

---

## Sample Logs
```
2026-02-26 05:18:28 [INFO] Starting Task 1 ingestion
2026-02-26 05:18:28 [INFO] Loaded 105 rows
2026-02-26 05:18:28 [INFO] Transformations complete.
2026-02-26 05:18:28 [INFO] --- Data Quality Report ---
2026-02-26 05:18:28 [INFO]   Total rows       : 105
2026-02-26 05:18:28 [INFO]   boolean_col values: True=48, False=41, NULL=16
2026-02-26 05:18:28 [INFO]   Validation PASSED ✓
2026-02-26 05:18:31 [INFO] SUCCESS: 105 rows loaded into task1_data_result

2026-02-26 05:13:52 [INFO] Starting Task 2 ingestion
2026-02-26 05:13:52 [INFO] Parameter extracted from filename: 2025-01-01
2026-02-26 05:13:52 [INFO] Extracted 600 rows
2026-02-26 05:13:52 [INFO]   Validation PASSED ✓
2026-02-26 05:13:54 [INFO] SUCCESS: 600 rows loaded into task2_data_result
```
