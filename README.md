# Walmart Lakehouse Data Pipeline

A complete end-to-end data engineering project built around Airflow, dbt, and Databricks for orchestrating and modeling Walmart-style retail data. The project loads transactional data into a PostgreSQL source layer, orchestrates ingestion and transformations through Airflow, and builds a curated analytics layer in Databricks using dbt.

## Overview

This repository demonstrates a realistic modern data platform pattern:

- Data is loaded from CSV files into PostgreSQL tables representing retail entities.
- Airflow schedules and orchestrates the end-to-end workflow.
- A Databricks job is triggered as part of the workflow for CDC/ingestion operations.
- dbt builds and tests a layered warehouse model:
  - Source data
  - Silver technical and business models
  - Gold fact and dimension tables
- Snapshots and tests help maintain logic quality and historical tracking.

The pipeline is designed around the following business domains:

- Customers
- Stores
- Products
- Employees
- Orders
- Order items

## Architecture

The project follows a medallion-style lakehouse architecture.

```text
CSV files / source datasets
          ↓
PostgreSQL raw tables
          ↓
Airflow DAG orchestration
          ↓
Databricks ingestion / CDC job
          ↓
dbt source freshness + transformations
          ↓
Silver layer (technical + business)
          ↓
Gold layer (facts + snapshots)
          ↓
Analytics / reporting consumption
```

## Tech Stack

- Python 3.12
- Apache Airflow 3.x
- Docker + Docker Compose
- dbt Core
- Databricks SQL / Lakehouse
- PostgreSQL
- Databricks SDK
- Pandas / Python for CSV loading utilities

## Project Structure

```text
.
├── dags/
│   ├── orchestrate.py
│   └── test.py
├── walmart_datapipeline/
│   └── walmart_dataset/
│       ├── ddl/
│       │   └── walmart_schema.sql
│       ├── data/
│       └── load_data.py
├── walmart_dbt/
│   ├── profiles.yml
│   ├── README.md
│   └── walmart_dp/
│       ├── analyses/
│       ├── macros/
│       ├── models/
│       │   ├── gold/
│       │   ├── silver_b/
│       │   ├── silver_t/
│       │   └── source/
│       ├── snapshots/
│       ├── tests/
│       ├── dbt_project.yml
│       └── target/
├── config/
├── Dockerfile
├── docker-compose.yaml
├── .env
├── pyproject.toml
├── requirement.txt
├── uv.lock
├── README.md
└── .gitignore
```

## Data Model

The transactional dataset is centered around six main tables:

- customers
- stores
- products
- employees
- orders
- order_items

Each table contains common metadata fields such as:

- created_timestamp
- updated_timestamp
- is_active

This structure supports incremental change tracking and analytical history reconstruction.

## dbt Layering

The dbt project under `walmart_dbt/walmart_dp` is organized into the following layers:

### Source layer

Defined in `models/source/sources.yml` and references the Databricks catalog/schema where raw warehouse tables live.

### Silver layer

The silver models are split into:

- `silver_t` for technical/cleaned layer tables
- `silver_b` for business-oriented joined model(s)

Examples include:

- `customers_t.sql`
- `orders_t.sql`
- `products_t.sql`
- `stores_t.sql`
- `employees_t.sql`
- `order_items_t.sql`
- `obt_b.sql`

These models normalise and prepare the data for downstream analytics.

### Gold layer

The gold layer contains:

- ephemeral staging models under `models/gold/ephemeral/`
- fact models such as `fact_orders.sql`
- snapshot-based dimensional tables under `snapshots/`

Examples:

- `dim_customers.yml`
- `dim_employees.yml`
- `dim_orders.yml`
- `dim_products.yml`
- `dim_stores.yml`

## Airflow Orchestration

The DAG in `dags/orchestrate.py` is responsible for the workflow sequence.

### Schedule

The pipeline is scheduled with:

```python
schedule="0 16 * * *"
```

This means the DAG runs daily at 16:00.

### Workflow order

The workflow executes in this order:

1. `ingest_cdc`
   - Triggers a Databricks Jobs run using the Databricks SDK.
2. `clean_up`
   - Removes stale dbt build artifacts from the target folder.
3. `source_freshness`
   - Runs `dbt source freshness`.
4. `silver_technical`
   - Runs `dbt run --select silver_t`
5. `silver_technical_tests`
   - Runs `dbt test --select silver_t`
6. `silver_business`
   - Runs `dbt run --select silver_b`
7. `silver_business_tests`
   - Runs `dbt test --select silver_b`
8. `gold_ephemeral`
   - Builds ephemeral gold staging models.
9. `gold_dimensions`
   - Executes dbt snapshots for dimension history.
10. `gold_facts`
    - Builds fact model(s) for reporting.

## Local Setup

### Prerequisites

Before running the project, ensure you have:

- Python 3.12+
- Docker and Docker Compose
- Access to a Databricks workspace and SQL warehouse
- A PostgreSQL connection string for the source data
- A working Databricks PAT token

### 1. Clone the repository

```bash
git clone https://github.com/janardhanpudi/walmart_dbt_airflow_Lakehouse.git
cd walmart_dbt_airflow_Lakehouse
```

### 2. Create and activate a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

Using pip:

```bash
pip install -r requirement.txt
```

Or using uv:

```bash
uv sync
```

### 4. Configure environment variables

The repository includes `.env` and Databricks profile configuration, but you should replace placeholders with your own values.

Key items to configure:

- Databricks host
- Databricks SQL warehouse HTTP path
- Databricks access token
- Databricks catalog/schema names
- PostgreSQL connection string for the source dataset

Example profile snippet:

```yaml
walmart_dp:
  target: dev
  outputs:
    dev:
      type: databricks
      host: YOUR_DATABRICKS_HOST
      http_path: YOUR_DATABRICKS_HTTP_PATH
      catalog: walmart
      schema: dbt_schema
      token: YOUR_DATABRICKS_TOKEN
      threads: 1
```

Important: Do not commit secrets or tokens to source control. Use environment variables or a secure secret manager.

## Running the Data Loader

The project includes a data-loading script for injecting CSV data into PostgreSQL.

```bash
python walmart_datapipeline/walmart_dataset/load_data.py
```

This script reads CSV files from the dataset folder and loads them into the corresponding raw tables.

## Running dbt

From the dbt project directory:

```bash
cd walmart_dbt/walmart_dp

dbt debug --profiles-dir ../
dbt run --profiles-dir ../
dbt test --profiles-dir ../
```

Useful commands:

```bash
dbt source freshness --profiles-dir ../
dbt snapshot --profiles-dir ../
dbt run --select silver_t --profiles-dir ../
dbt run --select models/gold/fact --profiles-dir ../
```

## Running Airflow with Docker

From the project root:

```bash
docker compose up -d --build
```

Then open:

```text
http://localhost:8080
```

Default Airflow UI credentials are usually:

- Username: `airflow`
- Password: `airflow`

If your local `.env` overrides these values, use the configured credentials instead.

## Data Quality and Testing

The dbt project includes validations to ensure key model quality rules are enforced.

Examples include:

- `not_null` checks
- `unique` checks
- model-specific business validation in the tests directory

This ensures critical columns such as primary keys and essential business fields remain valid.

## Features

- Daily workflow orchestration with Airflow
- Databricks job triggering via API
- Source freshness validation
- dbt test execution for quality assurance
- Incremental load handling
- Snapshot-based SCD style history tracking
- Layered medallion architecture
- Ready-to-extend retail analytics model

## Common Use Cases

- Retail KPI reporting
- Sales trend analysis
- Customer behavior analysis
- Store and product performance monitoring
- Monthly and daily order analytics
- Historical dimension tracking over time

## Notes

- The production workflow in `dags/orchestrate.py` uses a real Databricks workspace URL and job ID in the ingestion step and should be replaced with your own environment configuration before deployment.
- The project is structured as a demonstration and can be extended with additional ingestion sources, scheduling logic, alerting, and data governance controls.
- For security-sensitive deployments, prefer environment variables, vault-backed secrets, or Databricks secret scopes rather than hardcoded credential values.

## Next Enhancements

Potential improvements for this project include:

- CI/CD automation for dbt validation
- Slack or email alerts for failed Airflow tasks
- Additional marts for finance, operations, and HR analytics
- Partitioning and clustering for performance tuning
- Airflow variable-based configuration for all environment settings
- Full production deployment on a managed orchestration platform

## Conclusion

This project provides a practical, modern data stack implementation that combines orchestration, transformation, and lakehouse analytics in one cohesive workflow. It is a strong starting point for building production-grade data pipelines around retail analytics, operational dashboards, and decision-support workloads.
