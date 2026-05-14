# Retail Sales Intelligence Pipeline

An end-to-end data engineering project built to demonstrate production-grade ETL skills using real-world retail data.

![Python](https://img.shields.io/badge/Python-3.x-blue)
![PySpark](https://img.shields.io/badge/PySpark-4.1.1-orange)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)
![Great Expectations](https://img.shields.io/badge/Great%20Expectations-1.x-green)
![Airflow](https://img.shields.io/badge/Airflow-2.x-red)
![CI](https://github.com/Yashi-For-Now/retail-intelligence/actions/workflows/ci.yml/badge.svg)

## Live Dashboard
👉 [Retail Sales Intelligence Dashboard](https://public.tableau.com/app/profile/yashashwani.singh/viz/RetailIntelligencDashboard/RetailSalesIntelligenceDashboard)

## Project Overview

This pipeline ingests, profiles, transforms, validates and loads 1M+ rows of UK e-commerce data into a PostgreSQL star schema, with results visualised in a Tableau Public dashboard.

| Stage | Script | Description |
|---|---|---|
| 0 | `profile.py` | Statistical profiling of raw data |
| 1 | `ingest.py` | Batch CSV ingestion via Pandas |
| 2 | `transform.py` | PySpark cleaning & aggregations |
| 3 | `validate.py` | Great Expectations quality checks |
| 4 | `load.py` | PostgreSQL star schema load |
| 5 | `export_dashboard_data.py` | CSV exports for Tableau |
| 6 | `dags/retail_pipeline_dag.py` | Airflow orchestration |

## Tech Stack

- **PySpark 4.1.1** — core transformation engine
- **Pandas** — ingestion & preprocessing
- **PostgreSQL 16** — data warehouse (star schema)
- **Great Expectations** — data quality validation
- **Apache Airflow** — pipeline orchestration
- **GitHub Actions** — CI/CD
- **Tableau Public** — dashboard & visualisation

## Dataset

**Online Retail II UCI** — Kaggle  
1,067,371 rows · 8 columns · UK e-commerce transactions (2009–2011)

## Key Findings from Profiling

| Metric | Value |
|---|---|
| Total records | 1,067,371 |
| Duplicate rows | 34,335 (3.22%) |
| Null Customer IDs | 243,007 (22.77%) |
| Cancellation rate | 1.83% |
| Negative quantities | 22,950 |
| Sub-penny prices | 6,225 |
| Dirty data removed | ~8% |

These findings directly informed the cleaning strategy in `transform.py`.

## Star Schema Design
fact_sales
├── customer_id  → dim_customer
├── stock_code   → dim_product
├── date_id      → dim_date
└── region_id    → dim_region

**Why star over snowflake?**  
Star schema avoids multi-table joins for aggregation queries. For OLAP workloads like dashboard queries, the denormalised structure gives significantly better read performance.

## Pipeline Results

| Table | Rows Loaded |
|---|---|
| dim_customer | 5,879 |
| dim_product | 4,916 |
| dim_date | 37,212 |
| dim_region | 43 |
| fact_sales | 1,007,895 |

## Data Quality — Great Expectations

8 expectations enforced on every run:
- Required columns exist
- No nulls in critical columns (Invoice, StockCode, Quantity, Price, Revenue)
- Quantity between 1 and 100,000
- Price minimum £0.01
- Revenue minimum £0.01
- Country unique count between 1–200
- Revenue integrity: `Quantity × Price = Revenue` within £0.01 tolerance

Pipeline exits with code 1 on any failure — blocks CI/CD from proceeding.

## Design Decisions

**PySpark over Pandas for transform** — dataset exceeds 1M rows. PySpark processes lazily and in parallel across all CPU cores, making it the right tool for production-scale transforms.

**Batch ingestion** — CSV read in 50K row chunks to protect memory on constrained environments.

**Surrogate keys** — dimension tables use auto-generated SERIAL primary keys, not natural keys. This handles slowly changing dimensions and protects fact table integrity.

**Cancellations filtered at source** — invoices starting with 'C' represent returns, not sales. Including them would corrupt revenue aggregations.

## Setup

### Prerequisites
- Python 3.x
- Java JDK 11+
- PostgreSQL 16
- PySpark (with Hadoop winutils on Windows)

### Installation

```bash
git clone https://github.com/YOURUSERNAME/retail-intelligence.git
cd retail-intelligence
python -m venv venv
venv\Scripts\activate      # Windows
pip install -r requirements.txt
```

### Environment Variables

Create a `.env` file in the project root:
DB_HOST=localhost
DB_PORT=5432
DB_NAME=retail_db
DB_USER=postgres
DB_PASSWORD=yourpassword

### Run the Pipeline

```bash
python profile.py
python ingest.py
python transform.py
python validate.py
python load.py
python export_dashboard_data.py
```

### Run Tests

```bash
pytest tests/
```

## Project Structure
retail-intelligence/
├── data/
│   ├── raw/                  ← source CSV (not committed)
│   └── processed/            ← parquet & CSV outputs
├── dags/
│   └── retail_pipeline_dag.py
├── tests/
├── .github/
│   └── workflows/
│       └── ci.yml
├── profile.py
├── ingest.py
├── transform.py
├── validate.py
├── load.py
├── export_dashboard_data.py
├── requirements.txt
└── .env                      ← not committed

## Author

**Yashashwani Singh**  