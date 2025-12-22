# Data Engineer Portfolio – EventStream Lakehouse

## Overview
This project demonstrates an end-to-end data engineering pipeline built on AWS.
It ingests synthetic clickstream events, transforms them into a clean and curated
analytics model, and exposes them for SQL-based analysis.

## How to Run (Local)

### Prerequisites
- Python 3.10+
- Git

### Setup
```bash
git clone <repo-url>
cd data-engineer-portfolio

python -m venv .venv
source .venv/Scripts/activate
pip install -r requirements.txt


# Generate Sample Events:
python ingestion/generate_events.py \
  --out data/raw/events.ndjson \
  --rows 10000 \
  --seed 42 \
  --bad-rate 0.01


## Architecture (High Level)
- Python event generator (local)
- Amazon S3 data lake (raw / clean / curated)
- AWS Glue for transformations and data quality
- dbt for analytics modeling
- Athena (and/or Redshift) for querying
- Step Functions for orchestration
- CloudWatch for monitoring

## Repository Structure
