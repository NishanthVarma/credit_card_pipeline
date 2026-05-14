# Credit Card Transaction Pipeline

A production-style end-to-end data engineering pipeline built to simulate real-world fraud transaction data processing. This project demonstrates a modern cloud data warehouse architecture using Python, PySpark, AWS S3, Snowflake, dbt, and Apache Airflow — orchestrated end-to-end via Apache Airflow running on Docker Compose.

---

## Architecture

![Credit Card Transaction Pipeline Architecture](assets/Capstone_Project_architecture.png)

> Apache Airflow (Docker Compose) orchestrates all pipeline stages: Bronze ingestion → Silver transformation → dbt Gold build. All tables — dimensions and fact — are built through dbt into a clean star schema in Snowflake.

---

## Dataset

- **Source:** [Kaggle — Credit Card Fraud Detection](https://www.kaggle.com/datasets/kartik2112/fraud-detection)
- **Size:** ~1.85 million rows (fraudTrain.csv + fraudTest.csv combined)
- **Content:** Simulated credit card transactions including merchant details, customer demographics, transaction amounts, and fraud labels

---

## Pipeline Walkthrough

### 1. Data Generation (`dim_generation/`)
Raw Kaggle CSVs are used as the fact source. `dim_faker.py` reads the source data and generates normalized dimension tables:
- `dim_customer` — deduplicated on credit card number
- `dim_merchant` — deduplicated on merchant name, enriched with latest lat/long
- `dim_card` — Faker-enriched card details (expiry date, issue date, credit limit)
- `fact_transactions` — enriched with `cust_uuid` and `merch_uuid` foreign keys

All outputs are written as CSVs to the `landing/` folder, simulating raw OLTP CSV dumps.

### 2. Bronze Layer (`bronze/`)
PySpark reads CSVs from `landing/` and writes Parquet to S3 — raw format conversion only, no cleaning.
- Dims written flat → `s3://cc-transaction-pipeline/brz/brz_dim_*/`
- Fact partitioned by `year/month/day` using Hive-style partitioning for partition pruning

### 3. Silver Layer (`silver/`)
Cleaning and type casting happens here via PySpark:
- `slv_dims.py` — casts data types (zip→int, lat/long→double, dates→DateType), strips `"fraud_"` merchant prefix
- `slv_fact.py` — selects relevant columns, casts `cc_num` to string, written flat to S3 (no partitioning — feeds Snowflake directly)

### 4. Gold Layer — Snowflake + dbt (`cc_pipeline_dbt/`)

**Snowflake Setup**

Silver Parquet files on S3 are loaded into Snowflake using a storage integration and external stage:
- A Storage Integration + IAM trust relationship connects Snowflake to the S3 `slv/` prefix
- Tables are created via `INFER_SCHEMA` + `USING TEMPLATE` to automatically derive column definitions from Parquet metadata
- `COPY INTO` loads all Silver Parquet files into a transient staging schema (`cc_pipeline_staging.raw`)

**dbt Transformation**

dbt builds a clean star schema in `cc_pipeline_gold.gold` through two layers:

*Staging (views)* — `stg_dim_*` models sit on top of raw Snowflake tables, aliasing quoted uppercase column names (produced by `INFER_SCHEMA`) to clean lowercase equivalents.

*Gold (tables)* — Final dimension and fact tables with business logic applied:
- `dim_card` — adds `is_top_card` flag (credit limit ≥ 10,000) via CASE statement
- `dim_customer` — derives `age` from `dob` using `DATEDIFF`
- `dim_merchant` — cleaned passthrough from staging
- `fct_transactions` — denormalized fact table joining all dims; adds first name, last name, merchant name, card provider, and credit limit

dbt tests (29 schema tests) validate primary key uniqueness, not-null constraints, and referential integrity across all models.

### 5. Orchestration (`airflow/`)
Full pipeline orchestrated via a daily Airflow DAG:
```
bronze >> silver >> dbt
```
- **bronze** and **silver** tasks run PySpark scripts via `subprocess.run`
- **dbt** task runs `dbt build` via `@task.bash` — executes all models and tests in a single command
- Retries: 3 attempts with 5-minute delay
- Backfill supported via Airflow UI
- Runs inside Docker Compose with a custom image (Java + PySpark + dbt-snowflake baked in)

---

## Tech Stack

| Layer | Tool |
|---|---|
| Data Generation | Python, Faker |
| Ingestion & Transformation | PySpark |
| Storage | AWS S3 (Parquet, Medallion Architecture) |
| Warehouse | Snowflake |
| Transformation (Gold) | dbt Core |
| Orchestration | Apache Airflow (Docker Compose) |
| Language | Python 3.13 |

---

## Project Structure

```
CC Transaction Pipeline/
├── assets/
│   └── Capstone_Project_architecture.png
├── cfg/
│   └── config_template.py            # Template — copy to config.py and fill values
├── dim_generation/
│   └── dim_faker.py                  # Generates dimension CSVs from source data
├── bronze/
│   └── brz.py                        # PySpark ingestion to S3 Bronze layer
├── silver/
│   ├── slv_dims.py                   # Silver dimension cleaning
│   └── slv_fact.py                   # Silver fact cleaning
├── airflow/
│   ├── dags/
│   │   └── my_dag.py                 # Airflow DAG (bronze >> silver >> dbt)
│   ├── Dockerfile                    # Custom Airflow image with Java + PySpark + dbt
│   ├── docker-compose.yaml           # Airflow local setup
│   └── requirements.txt              # Python dependencies (includes dbt-snowflake)
├── cc_pipeline_dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_dim_card.sql
│   │   │   ├── stg_dim_customer.sql
│   │   │   └── stg_dim_merchant.sql
│   │   └── gold/
│   │       ├── dim_card.sql
│   │       ├── dim_customer.sql
│   │       ├── dim_merchant.sql
│   │       └── fct_transactions.sql
│   ├── dbt_project.yml
│   └── profiles.yml                  # Gitignored — Snowflake credentials
├── landing/                          # Generated CSVs (gitignored)
├── source/                           # Raw Kaggle CSVs (gitignored)
└── README.md
```

---

## Setup & How to Run

### Prerequisites
- Python 3.13
- Java JDK 17
- Docker Desktop
- AWS CLI configured (`ap-south-1`)
- Snowflake account (Enterprise or Trial)

### Configuration
1. Copy `cfg/config_template.py` to `cfg/config.py`
2. Fill in your AWS credentials and local paths
3. Set `PROJECT_ROOT` environment variable if running inside Docker
4. In `cc_pipeline_dbt/`, copy `profiles.yml` from the template and fill in your Snowflake credentials

### Snowflake Setup (one-time)
Run the following in Snowflake in order:
1. Create staging and gold databases + schemas
2. Create a Storage Integration and update your IAM role's trust policy with the Snowflake ARN
3. Create an external stage pointing to `s3://cc-transaction-pipeline/slv/`
4. Create raw tables using `INFER_SCHEMA` + `USING TEMPLATE` from the external stage
5. Run `COPY INTO` to load Silver Parquet files into the staging schema

### Running Locally (step-by-step)
```bash
# Step 1 — Generate dimension data
python dim_generation/dim_faker.py

# Step 2 — Run Bronze ingestion
python bronze/brz.py

# Step 3 — Run Silver transformation
python silver/slv_dims.py
python silver/slv_fact.py

# Step 4 — Run dbt (Gold layer)
cd cc_pipeline_dbt
dbt build --profiles-dir .
```

### Running via Airflow (full pipeline)
```bash
cd airflow
docker compose up --build -d
# Open http://localhost:8080
# Trigger the DAG manually or wait for the daily schedule
# DAG runs: bronze → silver → dbt build (all models + tests)
```

---

## Key Design Decisions

- **Parquet over Delta Lake** — Gold layer is Snowflake which reads Parquet natively. Delta Lake benefits (ACID, time travel, MERGE) apply in Spark/lakehouse-native stacks like Databricks; they add no value when the final destination is a cloud data warehouse.
- **No partitioning on Silver fact** — Silver feeds Snowflake directly and is never queried via Spark, so Hive-style partitioning adds no value here.
- **Hive-style partitioning on Bronze fact** — enables partition pruning for downstream Spark reads from S3.
- **PII handling in Silver** — data cleaning and PII drops belong in the transformation layer, not in data generation scripts. Bronze is always raw.
- **INFER_SCHEMA for Snowflake table creation** — avoids manually defining column types; derives the schema directly from Parquet metadata. Requires explicit column aliasing in dbt staging models to handle the uppercase column names it produces.
- **Transient staging DB in Snowflake** — raw tables loaded via `COPY INTO` don't need time travel or fail-safe (they can always be reloaded from S3), so a transient database avoids unnecessary storage costs.
- **dbt staging layer as a case-sensitivity fix** — `INFER_SCHEMA` produces quoted uppercase column names. Staging models alias these to clean lowercase equivalents so all downstream Gold models reference consistent, unquoted identifiers.
- **`dbt build` as the Airflow task** — runs models + tests in dependency order in a single command, making the DAG task simple and self-validating.
