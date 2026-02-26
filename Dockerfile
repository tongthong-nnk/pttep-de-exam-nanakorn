FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir \
    pandas \
    pyarrow \
    google-cloud-bigquery \
    pytz \
    openpyxl

# Copy scripts and data
COPY scripts/ ./scripts/
COPY data/ ./data/

# Create logs directory
RUN mkdir -p /app/logs

# Default command
CMD ["python", "scripts/task1_ingestion.py"]
