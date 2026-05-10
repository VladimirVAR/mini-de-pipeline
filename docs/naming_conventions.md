# Naming Conventions

This document describes the naming patterns used in the **Mini DE Pipeline** project. It documents the conventions that already exist in the repository and should be followed when extending the project.

---

## General Principles

* Use **English** for object names, columns, files, and code identifiers.
* Use **snake_case** with lowercase letters and underscores.
* Use descriptive names that explain the object purpose.
* Use schema-qualified names in documentation when clarity is needed, for example `raw.iso_deliverables` or `warehouse.vw_deliverables_current`.

---

## Schema Naming

| Schema      | Purpose                                           |
| ----------- | ------------------------------------------------- |
| `raw`       | Source-level data and ingestion metadata.         |
| `staging`   | Cleaned, typed, and normalized intermediate data. |
| `warehouse` | Analytics-ready snapshot tables and views.        |
| `dbt_iso`   | dbt-managed staging models.                       |

---

## Object Naming

### Raw Layer

Raw objects use source-oriented or technical metadata names.

| Pattern           | Example            | Meaning                              |
| ----------------- | ------------------ | ------------------------------------ |
| `<source_entity>` | `iso_deliverables` | Raw source entity.                   |
| `load_batches`    | `load_batches`     | Batch metadata and execution status. |

---

### Staging Layer

Staging objects describe cleaned or normalized versions of raw data.

| Pattern                                | Example                     | Meaning                               |
| -------------------------------------- | --------------------------- | ------------------------------------- |
| `<entity>_clean`                       | `iso_deliverables_clean`    | Main cleaned staging table.           |
| `<entity_singular>_<attribute_plural>` | `iso_deliverable_languages` | Exploded multi-value attribute table. |
| `<entity_singular>_relations`          | `iso_deliverable_relations` | Exploded relationship table.          |

---

### Warehouse Layer

Warehouse objects use analytical role prefixes.

| Pattern                                | Example                                  | Meaning                                      |
| -------------------------------------- | ---------------------------------------- | -------------------------------------------- |
| `fact_<entity>_snapshot`               | `fact_deliverable_snapshot`              | Main batch-level analytical snapshot.        |
| `bridge_<entity>_snapshot_<attribute>` | `bridge_deliverable_snapshot_languages`  | Bridge table for multi-value attributes.     |
| `factless_<entity>_relation_snapshot`  | `factless_deliverable_relation_snapshot` | Relationship table without numeric measures. |
| `vw_<entity_plural>_current`           | `vw_deliverables_current`                | View exposing the latest successful batch.   |

---

### dbt Models

| Pattern                                    | Example                         | Meaning                                    |
| ------------------------------------------ | ------------------------------- | ------------------------------------------ |
| `stg_<entity>`                             | `stg_iso_deliverables_clean`    | dbt staging model.                         |
| `stg_<entity_singular>_<attribute_plural>` | `stg_iso_deliverable_languages` | dbt staging model for exploded attributes. |

---

## Column Naming

| Pattern               | Example                     | Meaning                                         |
| --------------------- | --------------------------- | ----------------------------------------------- |
| `raw_row_id`          | `raw_row_id`                | Technical row identifier used across layers.    |
| `load_batch_id`       | `load_batch_id`             | Batch lineage key.                              |
| `source_row_num`      | `source_row_num`            | Original row number from the CSV source.        |
| `<entity>_id`         | `deliverable_id`            | Business or source identifier.                  |
| `related_<entity>_id` | `related_deliverable_id`    | Identifier of a related entity.                 |
| `*_raw`               | `languages_raw`             | Preserved raw or partially parsed source value. |
| `*_code`              | `language_code`, `ics_code` | Normalized code value.                          |
| `*_date`              | `publication_date`          | Date column.                                    |
| `*_year`              | `publication_year`          | Year extracted from a date.                     |
| `*_type`              | `relation_type`             | Type or category field.                         |
| `is_*`                | `is_published`              | Boolean state flag.                             |
| `has_*`               | `has_title_en`              | Boolean presence flag.                          |
| `*_at`                | `ingested_at`               | Timestamp column.                               |
| `*_count`             | `loaded_row_count`          | Count metric.                                   |
| `source_*`            | `source_file`               | Source metadata.                                |
| `error_*`             | `error_message`             | Error tracking information.                     |

---

## File Naming

| Pattern                    | Example                              | Meaning                           |
| -------------------------- | ------------------------------------ | --------------------------------- |
| `create_<object>.sql`      | `create_iso_deliverables.sql`        | Creates a database object.        |
| `load_<object>.sql`        | `load_fact_deliverable_snapshot.sql` | Loads data into a target object.  |
| `add_<object>.sql`         | `add_basic_constraints.sql`          | Adds supporting database objects. |
| `run_<pipeline>.py`        | `run_iso_pipeline.py`                | Pipeline runner script.           |
| `load_<entity>_<layer>.py` | `load_iso_deliverables_raw.py`       | Python loader module.             |

---

## Not Used in This Project

The project currently does **not** use these common DWH patterns:

* `dim_` dimension tables.
* Surrogate keys with the `_key` suffix.
* Stored procedures named `load_<layer>`.
* `dwh_` technical column prefix.

Instead, this project is based on `raw_row_id`, `load_batch_id`, snapshot tables, bridge tables, dbt staging models, and a current-state view.
