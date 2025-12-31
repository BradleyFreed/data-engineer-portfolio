# Data Engineer Portfolio – Event Analytics Pipeline

## Overview

This project demonstrates an end-to-end, cloud-native data engineering pipeline
using AWS services. It simulates clickstream-style event data, ingests it into a
data lake on Amazon S3, catalogs it with AWS Glue, and transforms it into a
clean, analytics-optimized format using Amazon Athena.

The pipeline follows modern data lake best practices, including schema-on-read,
decoupled storage and compute, partitioned data, and columnar file formats.

---

## Architecture Overview

The pipeline follows a layered data lake architecture:
```text
Python (event generator)
  → Amazon S3 (raw zone, NDJSON)
  → AWS Glue Data Catalog (schema-on-read)
  → Amazon Athena (CTAS transformation)
  → Amazon S3 (clean zone, Parquet)
```
Future extensions may include dbt for analytics modeling and Glue ETL jobs for
advanced data quality and transformations.

---

## Raw Data Layer

Synthetic event data is generated locally and written to Amazon S3 as raw,
immutable newline-delimited JSON (NDJSON). Data is stored using partition-style
prefixes to support efficient downstream querying.

Example S3 layout:
```text
s3://brad-data-engineer-portfolio-raw/
└── events/
    └── dt=YYYY-MM-DD/
        └── events.ndjson
```
An AWS Glue crawler catalogs the raw data and registers a table in the Glue Data
Catalog. The partition key (`dt`) is derived from the S3 path rather than the file
contents.

---

## Clean Data Layer

Raw JSON data is transformed into a clean, analytics-optimized format using
Amazon Athena with a CREATE TABLE AS SELECT (CTAS) query.

During this step:
- ISO-8601 timestamp strings are cast to proper timestamp types
- Invalid records are filtered
- Data is written in Parquet format with Snappy compression
- Existing date partitions are preserved

Example clean S3 layout:
```text
s3://brad-data-engineer-portfolio-clean/
└── events/
    └── dt=YYYY-MM-DD/
        └── *.parquet
```
This significantly reduces query cost and improves performance compared to
querying raw JSON directly.

“The clean layer supports incremental loads by appending new date partitions without rewriting existing data.”
---

## How to Run

### Prerequisites
- Python 3.10+
- Git
- AWS CLI configured with appropriate permissions

### Setup

```bash
git clone https://github.com/BradleyFreed/data-engineer-portfolio.git
cd data-engineer-portfolio

python -m venv .venv
source .venv/Scripts/activate
pip install -r requirements.txt
