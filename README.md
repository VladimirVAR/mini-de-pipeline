# Mini DE Pipeline

A modular batch data pipeline project built with Python, PostgreSQL, Docker, SQL, and dbt.

## Project Goal

This project demonstrates a clean and extensible data engineering workflow built around a real CSV source and a layered warehouse model.

The current version focuses on:

- ingesting ISO Open Data metadata from a CSV file,
- preserving source loads as identifiable batches,
- transforming raw rows into typed staging models,
- exploding multi-valued fields into child models,
- building a warehouse snapshot layer,
- exposing a current-state view over the latest snapshot,
- validating consistency across pipeline layers,
- introducing a dbt-based transformation layer for staging.

---

## Current Version Scope

The current implementation supports:

- **source type**: CSV file
- **input dataset**: ISO Open Data (`iso_deliverables_metadata`)
- **batch metadata**: `raw.load_batches`
- **raw layer**: `raw.iso_deliverables`

### Legacy pipeline objects

- **staging main table**: `staging.iso_deliverables_clean`
- **staging child tables**:
  - `staging.iso_deliverable_languages`
  - `staging.iso_deliverable_ics`
  - `staging.iso_deliverable_relations`
- **warehouse snapshot tables**:
  - `warehouse.fact_deliverable_snapshot`
  - `warehouse.bridge_deliverable_snapshot_languages`
  - `warehouse.bridge_deliverable_snapshot_ics`
  - `warehouse.factless_deliverable_relation_snapshot`
- **current-state view**: `warehouse.vw_deliverables_current`

### dbt layer (current migration state)

The project now includes a dedicated dbt subproject that reads from the existing `raw` layer and builds a dbt-managed staging layer in a separate schema.

Current dbt models:

- `dbt_iso.stg_iso_deliverables_clean`
- `dbt_iso.stg_iso_deliverable_languages`
- `dbt_iso.stg_iso_deliverable_ics`
- `dbt_iso.stg_iso_deliverable_relations`

Current dbt coverage:

- source definition for `raw.iso_deliverables`
- staging models built as dbt views
- column-level data tests
- generated dbt docs for the staging graph

### Execution / validation

- **legacy execution**: Python pipeline runner
- **dbt execution**: `dbt run`, `dbt test`, `dbt docs generate`
- **database**: PostgreSQL in Docker
- **validation**: legacy batch-level checks + dbt staging tests

---

## Architecture

### High-Level Flow

```text
CSV source
  -> raw load batch metadata
  -> raw landing table
  -> legacy SQL/Python staging + warehouse pipeline
  -> current-state analytical view

CSV source
  -> raw landing table
  -> dbt source(raw.iso_deliverables)
  -> dbt staging models in schema dbt_iso
  -> dbt tests + docs
```

### Data Layers

#### raw

- stores source data as close to the original as possible
- preserves each file load as a distinct `load_batch_id`
- supports both `full_refresh` and `append` semantics
- keeps the source recoverable and replayable

#### staging

- applies safe type casting and basic cleansing
- derives helper fields such as publication year and title flags
- preserves row-level traceability
- explodes multi-valued fields such as languages, ICS codes, and deliverable relationships
- is now implemented both in the legacy SQL pipeline and in the new dbt staging layer

#### warehouse

- stores one snapshot per batch in curated tables
- supports historical batch-level analysis
- exposes `warehouse.vw_deliverables_current` as the latest snapshot view
- supports downstream analytical queries on both current and historical data
- is still driven by the legacy SQL/Python pipeline in the current version

---

## Load Modes and Write Methods

### Load modes

- **`full_refresh`**: drops and recreates pipeline objects, then loads a fresh batch
- **`append`**: preserves prior batches and adds a new snapshot batch

### Write methods

- **`executemany`**: row-based batch insert into the raw layer
- **`copy`**: bulk load into the raw layer using PostgreSQL `COPY`

These two toggles are independent:

- `load_mode` controls **pipeline semantics**
- `load_method` controls **raw ingestion method**

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
│   ├── source_to_target_iso_deliverables.md
│   └── showcase_queries.md
│
├── sql/
│   ├── analytics/
│   │   └── showcase_queries.sql
│   ├── ddl/
│   │   ├── create_schemas.sql
│   │   ├── add_basic_constraints.sql
│   │   └── create_supporting_indexes.sql
│   ├── raw/
│   │   ├── create_load_batches.sql
│   │   └── create_iso_deliverables.sql
│   ├── staging/
│   │   ├── create_iso_deliverables_clean.sql
│   │   ├── create_iso_deliverable_languages.sql
│   │   ├── create_iso_deliverable_ics.sql
│   │   ├── create_iso_deliverable_relations.sql
│   │   ├── load_iso_deliverables_clean.sql
│   │   ├── load_iso_deliverable_languages.sql
│   │   ├── load_iso_deliverable_ics.sql
│   │   └── load_iso_deliverable_relations.sql
│   └── warehouse/
│       ├── create_fact_deliverable_snapshot.sql
│       ├── create_bridge_deliverable_snapshot_languages.sql
│       ├── create_bridge_deliverable_snapshot_ics.sql
│       ├── create_factless_deliverable_relation_snapshot.sql
│       ├── create_vw_deliverables_current.sql
│       ├── load_fact_deliverable_snapshot.sql
│       ├── load_bridge_deliverable_snapshot_languages.sql
│       ├── load_bridge_deliverable_snapshot_ics.sql
│       └── load_factless_deliverable_relation_snapshot.sql
│
├── src/
│   ├── __init__.py
│   ├── common/
│   │   ├── __init__.py
│   │   ├── logger.py
│   │   ├── pipeline_config.py
│   │   └── validation.py
│   ├── loaders/
│   │   ├── __init__.py
│   │   └── load_iso_deliverables_raw.py
│   └── pipeline/
│       ├── __init__.py
│       └── run_iso_pipeline.py
│
└── dbt_iso/
    └── mini_de_pipeline/
        ├── dbt_project.yml
        ├── models/
        │   ├── sources.yml
        │   └── staging/
        │       ├── staging_properties.yml
        │       ├── stg_iso_deliverables_clean.sql
        │       ├── stg_iso_deliverable_languages.sql
        │       ├── stg_iso_deliverable_ics.sql
        │       └── stg_iso_deliverable_relations.sql
        ├── macros/
        ├── seeds/
        ├── snapshots/
        └── tests/
```

---

## Input Data

The pipeline expects the following source file:

`data/landing/iso_deliverables_metadata.csv`

This file is not stored in Git and should be placed locally in the landing directory.

---

## Run the Legacy Pipeline

Run the pipeline from the project root:

```bash
python -m src.pipeline.run_iso_pipeline
```

Example `.env` values:

```env
POSTGRES_DB=de_project
POSTGRES_USER=de_user
POSTGRES_PASSWORD=de_password
POSTGRES_PORT=5433
POSTGRES_HOST=localhost

PIPELINE_LOAD_MODE=full_refresh
PIPELINE_LOAD_METHOD=executemany
```

---

## Run the dbt Staging Layer

Change into the dbt project directory:

```bash
cd dbt_iso/mini_de_pipeline
```

Run the dbt staging models:

```bash
dbt run
```

Run the dbt staging tests:

```bash
dbt test
```

Generate local dbt docs:

```bash
dbt docs generate
dbt docs serve
```

The current dbt target schema is expected to be separate from the raw and legacy staging schemas.

---

## Example Analytical Queries

The project includes a small showcase query set demonstrating:

- current-state analytics from the latest snapshot,
- distribution by deliverable type and committee,
- exploded child entities such as languages and ICS codes,
- lineage relationships between deliverables,
- snapshot history across load batches.

See:

- `sql/analytics/showcase_queries.sql`
- `docs/showcase_queries.md`

Examples include:

- current published vs withdrawn summary,
- top owner committees,
- top language codes / ICS codes,
- replacement lineage examples,
- snapshot history by batch.

---

## Source-to-Target Design

A source-to-target contract is documented in:

`docs/source_to_target_iso_deliverables.md`

This document defines:

- source schema,
- raw target schema,
- staging target schema,
- warehouse snapshot targets,
- column mapping,
- transformation rules,
- batch semantics.

---

## Tech Stack

- Python
- PostgreSQL
- Docker
- SQL
- dbt Core
- pandas
- psycopg
- python-dotenv

---

## Why This Project Matters

This project is designed to demonstrate practical data engineering fundamentals:

- source-to-target design
- separate raw, staging, and warehouse concerns
- preserve source loads as batches
- reproducible local environment
- support both full refresh and append semantics
- compare executemany vs copy ingestion methods
- safe transformation logic
- validation as part of the pipeline
- modular project structure ready for extension
- build a snapshot-based warehouse model
- expose a current-state analytical view
- introduce dbt-based transformation, testing, and documentation workflows

---

## Current Roadmap

The current migration status is:

- raw ingestion: handled by Python
- legacy staging: implemented in SQL / PostgreSQL
- dbt staging: implemented and tested
- warehouse dbt migration: planned next
- Airflow orchestration: planned after dbt warehouse migration
- additional source formats (for example JSONL / Parquet): planned future extension

---

## Author

Vladimir V.
