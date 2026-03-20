# Mini DE Pipeline

A modular batch data pipeline project built with Python, PostgreSQL, Docker, and SQL.

## Project Goal

This project demonstrates a clean and extensible data engineering workflow:

- ingest data from a source file
- load raw data into PostgreSQL
- transform raw data into a clean staging layer
- build a curated warehouse layer
- validate the transformed output
- run the pipeline through a single orchestration entry point

The long-term goal is to evolve this project into a reusable pipeline architecture that can support multiple input types, transformation layers, and output targets.

---

## Current Version Scope

The current implementation supports:

- **source type**: CSV file
- **input dataset**: ISO Open Data (`iso_deliverables_metadata`)
- **raw layer**: `raw.iso_deliverables`
- **staging layer**: `staging.iso_deliverables_clean`
- **warehouse layer**: `warehouse.iso_deliverables_core`
- **validation**: raw, staging, and warehouse consistency checks
- **execution**: Python pipeline runner
- **database**: PostgreSQL in Docker

---

## Architecture

### High-Level Flow

```text
CSV source
  -> raw layer
  -> staging layer
  -> warehouse layer
  -> validation
```

### Data Layers

#### raw

- stores source data as close to the original as possible
- column names are normalized to `snake_case`
- null-like source values are normalized before loading
- no heavy business cleansing logic is applied here

#### staging

- applies basic and safe transformations
- performs controlled type casting
- preserves row-level traceability
- acts as a rebuildable intermediate layer

#### warehouse

- provides a curated downstream layer
- contains consumer-friendly fields and simple derived attributes
- is rebuilt from `staging`

### Source-to-Target Design

A source-to-target contract is documented in:

`docs/source_to_target_iso_deliverables.md`

This document defines:

- source schema
- raw target schema
- staging target schema
- column mapping
- basic transformation rules

---

## Project Structure

```text
mini-de-pipeline/
│
├── .env.example
├── .gitignore
├── docker-compose.yml
├── README.md
├── requirements.txt
│
├── data/
│   ├── landing/
│   └── sample/
│
├── docs/
│   └── source_to_target_iso_deliverables.md
│
├── sql/
│   ├── ddl/
│   │   └── create_schemas.sql
│   ├── raw/
│   │   └── create_iso_deliverables.sql
│   ├── staging/
│   │   ├── create_iso_deliverables_clean.sql
│   │   ├── load_iso_deliverables_clean.sql
│   │   └── check_iso_deliverables_clean.sql
│   └── warehouse/
│       ├── create_iso_deliverables_core.sql
│       └── load_iso_deliverables_core.sql
│
└── src/
    ├── __init__.py
    ├── common/
    │   ├── __init__.py
    │   ├── logger.py
    │   ├── pipeline_config.py
    │   └── validation.py
    ├── ingestion/
    │   └── __init__.py
    ├── loaders/
    │   ├── __init__.py
    │   └── load_iso_deliverables_raw.py
    └── pipeline/
        ├── __init__.py
        └── run_iso_pipeline.py
```

---

## Tech Stack

- Python
- PostgreSQL
- Docker
- SQL
- pandas
- psycopg
- python-dotenv

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/VladimirVAR/mini-de-pipeline.git
cd mini-de-pipeline
```

### 2. Create a local `.env` file

Use `.env.example` as a template.

Example:

```env
POSTGRES_DB=de_project
POSTGRES_USER=de_user
POSTGRES_PASSWORD=de_password
POSTGRES_PORT=5433
POSTGRES_HOST=localhost
POSTGRES_CONTAINER_NAME=de_project_postgres

PIPELINE_LOAD_MODE=full_refresh
PIPELINE_LOAD_METHOD=executemany
```

### 3. Install Python dependencies

```bash
python -m pip install -r requirements.txt
```

### 4. Start PostgreSQL with Docker

```bash
docker compose up -d
```

### 5. Create database schemas and tables

Run these SQL files in PostgreSQL:

- `sql/ddl/create_schemas.sql`
- `sql/raw/create_iso_deliverables.sql`
- `sql/staging/create_iso_deliverables_clean.sql`
- `sql/warehouse/create_iso_deliverables_core.sql`

---

## Input Data

The current pipeline expects the following source file:

`data/landing/iso_deliverables_metadata.csv`

This file is not stored in Git and should be placed locally in the landing directory.

---

## Run the Pipeline

Run the full pipeline from the project root:

```bash
python -m src.pipeline.run_iso_pipeline
```

### Current pipeline flow

1. load pipeline configuration
2. reset layers in `full_refresh` mode
3. load source CSV into `raw.iso_deliverables`
4. load `staging.iso_deliverables_clean`
5. load `warehouse.iso_deliverables_core`
6. run validation checks
7. print pipeline status logs

---

## Supported Pipeline Modes

### Load Mode

Configured through:

```env
PIPELINE_LOAD_MODE=full_refresh
```

Currently supported:

- `full_refresh`
- `append` *(basic structure present, future refinement planned)*

### Load Method

Configured through:

```env
PIPELINE_LOAD_METHOD=executemany
```

Currently supported:

- `executemany`
- `copy` *(reserved for future implementation)*

---

## Validation

The pipeline currently validates:

- raw row count vs staging row count
- staging row count vs warehouse row count
- invalid raw `id` count vs `NULL` staging `id`
- invalid raw `publication_date` count vs `NULL` staging `publication_date`
- invalid raw `pages_en` count vs `NULL` staging `pages_en`
- staging `title_fr IS NULL` count vs warehouse `title_fr IS NULL` count
- warehouse `title_fr IS NULL` count vs `has_title_fr = false` count

Validation logic is implemented in:

`src/common/validation.py`

---

## Current Limitations

This is a completed V1 core implementation, with a few planned extensions still ahead. Current limitations include:

- only one source dataset is supported
- only CSV ingestion is currently implemented
- `copy` load method is not implemented yet
- `append` mode exists as a structural option and needs further refinement
- no API ingestion yet
- no database source ingestion yet
- no output/export sinks yet
- no Airflow or dbt integration yet

---

## Planned Next Steps

Planned improvements for the next iterations:

- add docstrings and light code cleanup
- improve append-mode logic
- implement `COPY`-based load method
- support additional file formats (`Parquet`, `JSONL`)
- support additional source types (`API`, database source)
- support output/export flows
- add orchestration tooling (future Airflow integration)
- add transformation framework support (future dbt integration)

---

## Why This Project Matters

This project is designed to demonstrate practical data engineering fundamentals:

- source-to-target design
- raw/staging/warehouse layer separation
- reproducible local environment
- controlled ingestion
- safe transformation logic
- validation as part of the pipeline
- modular project structure ready for extension

---

## Author

Vladimir V.
