# =============================================================================
# PROVIDER CONFIGURATION
# =============================================================================
provider "google" {
  project = "pttep-exam-tongthong"
  region  = "asia-southeast1"
}

# =============================================================================
# GOOGLE CLOUD STORAGE RESOURCES
# =============================================================================
resource "google_storage_bucket" "exam_bucket" {
  name          = "pttep-exam-data-nanakorn-2026"
  location      = "ASIA-SOUTHEAST1"
  force_destroy = true

  labels = {
    environment = "development"
    owner       = "nanakorn"
    project     = "pttep-exam"
  }
}

# =============================================================================
# BIGQUERY DATASET RESOURCES
# =============================================================================
resource "google_bigquery_dataset" "exam_dataset" {
  dataset_id = "exam_nanakorn"
  location   = "asia-southeast1"

  labels = {
    environment = "development"
    owner       = "nanakorn"
  }
}

# =============================================================================
# BIGQUERY TABLE RESOURCES
# =============================================================================

# Task 1: Staging Table for Storytelling Data
resource "google_bigquery_table" "task1_table" {
  dataset_id          = google_bigquery_dataset.exam_dataset.dataset_id
  table_id            = "task1_data_result"
  deletion_protection = false

  schema = <<EOF
[
  {"name": "row_id", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "integer_col", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "decimal_col", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "timestamp_col", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "boolean_col", "type": "BOOL", "mode": "NULLABLE"},
  {"name": "holiday_name", "type": "STRING", "mode": "NULLABLE"},
  {"name": "business_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "created_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"}
]
EOF
}

# Task 2: Result Table for Raw Data Migration
resource "google_bigquery_table" "task2_table" {
  dataset_id          = google_bigquery_dataset.exam_dataset.dataset_id
  table_id            = "task2_data_result"
  deletion_protection = false

  schema = <<EOF
[
  {"name": "date", "type": "DATE", "mode": "NULLABLE"},
  {"name": "parameter", "type": "DATE", "mode": "NULLABLE"},
  {"name": "asset", "type": "STRING", "mode": "NULLABLE"},
  {"name": "nomination", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "load_ts", "type": "TIMESTAMP", "mode": "NULLABLE"}
]
EOF
}
