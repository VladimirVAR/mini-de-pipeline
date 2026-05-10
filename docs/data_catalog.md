# Data Catalog for Warehouse Layer

## Overview

The Warehouse Layer is the analytics-ready layer of the Mini DE Pipeline. It stores ISO deliverable data as batch-level snapshots and exposes a current-state view based on the latest successfully loaded batch.

The warehouse model contains:

* one main snapshot fact table;
* bridge tables for multi-valued attributes such as languages and ICS codes;
* a factless relationship table for deliverable-to-deliverable relations;
* a current-state view for analytical queries.

---

## 1. `warehouse.fact_deliverable_snapshot`

**Purpose:** Stores one snapshot record per ISO deliverable per loaded batch. This is the main analytical table for historical and batch-level analysis.

| Column Name           |   Data Type | Description                                                                                |
| --------------------- | ----------: | ------------------------------------------------------------------------------------------ |
| `raw_row_id`          |      BIGINT | Technical row identifier inherited from the raw layer. Primary key of the snapshot record. |
| `load_batch_id`       |      BIGINT | Identifier of the ingestion batch that produced the snapshot.                              |
| `source_row_num`      |     INTEGER | Original row number from the source CSV file.                                              |
| `deliverable_id`      |      BIGINT | Parsed ISO deliverable identifier. Null when the source value cannot be safely converted.  |
| `deliverable_type`    |        TEXT | Type of ISO deliverable after basic cleanup.                                               |
| `supplement_type`     |        TEXT | Supplement type associated with the deliverable, if available.                             |
| `reference`           |        TEXT | ISO reference code or document reference.                                                  |
| `title_en`            |        TEXT | English title after cleanup.                                                               |
| `title_fr`            |        TEXT | French title after cleanup.                                                                |
| `publication_date`    |        DATE | Parsed publication date. Invalid source dates are stored as null.                          |
| `publication_year`    |     INTEGER | Year extracted from the publication date.                                                  |
| `edition`             |     INTEGER | Parsed edition number.                                                                     |
| `owner_committee`     |        TEXT | ISO committee responsible for the deliverable.                                             |
| `current_stage_raw`   |        TEXT | Original current stage value from the source.                                              |
| `current_stage_code`  |        TEXT | Digits-only stage code extracted from the raw stage value.                                 |
| `stage_family_code`   |        TEXT | First two digits of the stage code, used for high-level stage grouping.                    |
| `stage_subcode`       |        TEXT | Last two digits of the stage code, used for detailed stage classification.                 |
| `is_published`        |     BOOLEAN | Indicates whether the deliverable is in the published state.                               |
| `is_withdrawn`        |     BOOLEAN | Indicates whether the deliverable belongs to a withdrawn stage family.                     |
| `has_title_en`        |     BOOLEAN | Indicates whether an English title is available.                                           |
| `has_title_fr`        |     BOOLEAN | Indicates whether a French title is available.                                             |
| `pages_en`            |     INTEGER | Parsed number of English pages. Null when the source value is not numeric.                 |
| `scope_en_html`       |        TEXT | Original English scope field with HTML content preserved.                                  |
| `scope_en_text`       |        TEXT | Cleaned plain-text version of the English scope.                                           |
| `source_file`         |        TEXT | Name of the source file used for ingestion.                                                |
| `ingested_at`         | TIMESTAMPTZ | Timestamp when the raw row was ingested.                                                   |
| `warehouse_loaded_at` | TIMESTAMPTZ | Timestamp when the row was loaded into the warehouse snapshot table.                       |

---

## 2. `warehouse.bridge_deliverable_snapshot_languages`

**Purpose:** Stores one row per deliverable-language pair for each warehouse snapshot. This table normalizes the multi-valued language field from the source data.

| Column Name           |   Data Type | Description                                                                   |
| --------------------- | ----------: | ----------------------------------------------------------------------------- |
| `raw_row_id`          |      BIGINT | Technical identifier linking the language record to the deliverable snapshot. |
| `load_batch_id`       |      BIGINT | Identifier of the ingestion batch.                                            |
| `deliverable_id`      |      BIGINT | ISO deliverable identifier associated with the language.                      |
| `language_code`       |        TEXT | Normalized language code extracted from the source language list.             |
| `warehouse_loaded_at` | TIMESTAMPTZ | Timestamp when the bridge record was loaded into the warehouse.               |

---

## 3. `warehouse.bridge_deliverable_snapshot_ics`

**Purpose:** Stores one row per deliverable-ICS code pair for each warehouse snapshot. This table normalizes the multi-valued ICS classification field.

| Column Name           |   Data Type | Description                                                              |
| --------------------- | ----------: | ------------------------------------------------------------------------ |
| `raw_row_id`          |      BIGINT | Technical identifier linking the ICS record to the deliverable snapshot. |
| `load_batch_id`       |      BIGINT | Identifier of the ingestion batch.                                       |
| `deliverable_id`      |      BIGINT | ISO deliverable identifier associated with the ICS code.                 |
| `ics_code`            |        TEXT | Normalized ICS classification code extracted from the source list.       |
| `warehouse_loaded_at` | TIMESTAMPTZ | Timestamp when the bridge record was loaded into the warehouse.          |

---

## 4. `warehouse.factless_deliverable_relation_snapshot`

**Purpose:** Stores deliverable-to-deliverable relationships for each warehouse snapshot. This is a factless table because the relationship itself is the analytical event, without numeric measures.

| Column Name              |   Data Type | Description                                                                       |
| ------------------------ | ----------: | --------------------------------------------------------------------------------- |
| `raw_row_id`             |      BIGINT | Technical identifier linking the relationship record to the deliverable snapshot. |
| `load_batch_id`          |      BIGINT | Identifier of the ingestion batch.                                                |
| `deliverable_id`         |      BIGINT | Source deliverable identifier.                                                    |
| `related_deliverable_id` |      BIGINT | Related deliverable identifier extracted from the source relationship fields.     |
| `relation_type`          |        TEXT | Type of relationship. Valid values are `replaces` and `replaced_by`.              |
| `warehouse_loaded_at`    | TIMESTAMPTZ | Timestamp when the relationship record was loaded into the warehouse.             |

---

## 5. `warehouse.vw_deliverables_current`

**Purpose:** Exposes only the latest successfully loaded warehouse snapshot. This view is intended for current-state analytical queries where historical batch comparison is not required.

The view returns the same columns as `warehouse.fact_deliverable_snapshot`, filtered to the maximum `load_batch_id` from `raw.load_batches` where the batch status is `warehouse_loaded`.

---

## Supporting Metadata Table

### `raw.load_batches`

**Purpose:** Tracks each pipeline execution batch, including source file metadata, loading mode, loading method, batch status, row count, and error information.

| Column Name        |   Data Type | Description                                                                                       |
| ------------------ | ----------: | ------------------------------------------------------------------------------------------------- |
| `load_batch_id`    |   BIGSERIAL | Unique identifier of the pipeline load batch.                                                     |
| `source_name`      |        TEXT | Logical name of the data source.                                                                  |
| `source_file`      |        TEXT | Name or path of the ingested source file.                                                         |
| `file_hash_sha256` |        TEXT | SHA-256 hash of the source file, used for file-level traceability.                                |
| `load_mode`        |        TEXT | Pipeline loading mode. Valid values are `full_refresh` and `append`.                              |
| `load_method`      |        TEXT | Raw ingestion method. Valid values are `executemany` and `copy`.                                  |
| `status`           |        TEXT | Current batch status. Valid values are `started`, `raw_loaded`, `warehouse_loaded`, and `failed`. |
| `loaded_row_count` |     INTEGER | Number of rows loaded from the source file.                                                       |
| `started_at`       | TIMESTAMPTZ | Timestamp when the batch started.                                                                 |
| `finished_at`      | TIMESTAMPTZ | Timestamp when the batch finished.                                                                |
| `error_message`    |        TEXT | Error details if the batch failed.                                                                |

---

## Notes

* `load_batch_id` is the main lineage key across the pipeline.
* `raw_row_id` connects raw, staging, and warehouse records.
* The warehouse layer preserves historical snapshots by batch.
* `warehouse.vw_deliverables_current` provides the latest successful snapshot for current-state analysis.
* Language codes, ICS codes, and deliverable relationships are separated into dedicated tables because they can contain multiple values per deliverable.
