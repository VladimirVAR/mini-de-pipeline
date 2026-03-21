# ISO Deliverables: Source-to-Target Contract (V2)

## 1. Source

- Source name: `iso_deliverables_metadata`
- Source format: `CSV`
- Source file: `data/landing/iso_deliverables_metadata.csv`
- Source grain: `1 row = 1 deliverable record from the source snapshot`
- Observed columns: `16`

## 2. Runtime Semantics

### Load method
Controls **how raw rows are written**:
- `executemany`
- `copy`

### Load mode
Controls **how the pipeline behaves semantically**:
- `full_refresh` = truncate all target layers, then build one fresh batch
- `append` = create a new batch and preserve prior batches

## 3. Raw Layer

### 3.1 `raw.load_batches`
Grain:
- `1 row = 1 pipeline load batch`

Key fields:
- `load_batch_id`
- `source_name`
- `source_file`
- `file_hash_sha256`
- `load_mode`
- `load_method`
- `status`
- `loaded_row_count`
- `started_at`
- `finished_at`

### 3.2 `raw.iso_deliverables`
Grain:
- `1 row = 1 raw source row inside 1 load batch`

Primary identifiers:
- `raw_row_id`
- `load_batch_id`
- `source_row_num`

Goal:
- preserve source values as close to the original as practical
- keep batch traceability
- support append/snapshot history

## 4. Staging Layer

### 4.1 `staging.iso_deliverables_clean`
Grain:
- `1 row = 1 cleaned source row inside 1 load batch`

Purpose:
- safe type casting
- basic normalization
- lightweight business-safe derived fields

Important derived fields:
- `deliverable_id BIGINT`
- `publication_date DATE`
- `publication_year INTEGER`
- `edition INTEGER`
- `current_stage_code TEXT`
- `stage_family_code TEXT`
- `stage_subcode TEXT`
- `is_published BOOLEAN`
- `is_withdrawn BOOLEAN`
- `has_title_en BOOLEAN`
- `has_title_fr BOOLEAN`
- `scope_en_text TEXT`

### 4.2 `staging.iso_deliverable_languages`
Grain:
- `1 row = 1 cleaned source row + 1 language`

### 4.3 `staging.iso_deliverable_ics`
Grain:
- `1 row = 1 cleaned source row + 1 ICS code`

### 4.4 `staging.iso_deliverable_relations`
Grain:
- `1 row = 1 cleaned source row + 1 relation`

Relation types:
- `replaces`
- `replaced_by`

## 5. Warehouse Layer

### 5.1 `warehouse.fact_deliverable_snapshot`
Grain:
- `1 row = 1 cleaned source row in 1 warehouse snapshot batch`

Purpose:
- curated snapshot fact table
- preserve batch history
- support current-state view and future trend analysis

### 5.2 Bridge tables
- `warehouse.bridge_deliverable_snapshot_languages`
- `warehouse.bridge_deliverable_snapshot_ics`
- `warehouse.factless_deliverable_relation_snapshot`

### 5.3 Current-state view
- `warehouse.vw_deliverables_current`
- returns rows from the latest `warehouse_loaded` batch

## 6. Key Transformation Rules

- keep raw source columns in `TEXT` form in the raw layer
- normalize column names to `snake_case`
- cast `id` to `BIGINT` only when the value is numeric
- cast `publication_date` to `DATE` only when the value is a valid ISO date string
- cast `edition` and `pages_en` to integer only when numeric
- strip HTML tags from `scope_en` into `scope_en_text`
- explode `languages`, `ics_code`, `replaces`, and `replaced_by` into child tables
- preserve `source_file`, `load_batch_id`, and `source_row_num` for traceability

## 7. Validation Contract

Validation runs **per load batch** and checks:
- raw → staging row count parity
- staging → warehouse row count parity
- staging child row counts = warehouse child row counts
- no orphan rows in staging child tables
- malformed raw numeric/date fields are nullified as expected in staging
- `has_title_fr = false` is consistent with `title_fr IS NULL` in warehouse
