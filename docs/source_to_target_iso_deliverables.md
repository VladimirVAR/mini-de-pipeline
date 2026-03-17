# ISO Deliverables: Source-to-Target Contract

## 1. Source

- Source name: `iso_deliverables_metadata`
- Source format: `CSV`
- Source file: `data/landing/iso_deliverables_metadata.csv`
- Grain: `1 row = 1 deliverable`
- Observed row count: `80357`
- Observed column count: `16`

### Source columns

- `id`
- `deliverableType`
- `supplementType`
- `reference`
- `title.en`
- `title.fr`
- `publicationDate`
- `edition`
- `icsCode`
- `ownerCommittee`
- `currentStage`
- `replaces`
- `replacedBy`
- `languages`
- `pages.en`
- `scope.en`

## 2. Raw Target

- Target schema: `raw`
- Target table: `iso_deliverables`
- Load strategy: `append`
- Goal: preserve the source data as close to the original as possible

### Raw column mapping

- `id` -> `id`
- `deliverableType` -> `deliverable_type`
- `supplementType` -> `supplement_type`
- `reference` -> `reference`
- `title.en` -> `title_en`
- `title.fr` -> `title_fr`
- `publicationDate` -> `publication_date`
- `edition` -> `edition`
- `icsCode` -> `ics_code`
- `ownerCommittee` -> `owner_committee`
- `currentStage` -> `current_stage`
- `replaces` -> `replaces`
- `replacedBy` -> `replaced_by`
- `languages` -> `languages`
- `pages.en` -> `pages_en`
- `scope.en` -> `scope_en`

### Raw technical columns

- `source_file`
- `ingested_at`

## 3. Staging Target

- Target schema: `staging`
- Target table: `iso_deliverables_clean`
- Goal: create a cleaned and normalized table with basic type casting and safe transformations

### Proposed staging columns

- `id BIGINT`
- `deliverable_type TEXT`
- `supplement_type TEXT`
- `reference TEXT`
- `title_en TEXT`
- `title_fr TEXT`
- `publication_date DATE`
- `edition TEXT`
- `ics_code TEXT`
- `owner_committee TEXT`
- `current_stage TEXT`
- `replaces TEXT`
- `replaced_by TEXT`
- `languages_raw TEXT`
- `pages_en INTEGER`
- `scope_en TEXT`
- `source_file TEXT`
- `ingested_at TIMESTAMP`

## 4. Basic Transformation Rules

- Normalize column names to `snake_case`
- Do not apply heavy cleansing logic in the `raw` layer
- Apply basic and safe transformations in the `staging` layer
- Cast `publication_date` to `DATE`; if parsing fails, store `NULL`
- Cast `pages_en` to `INTEGER`; if parsing fails, store `NULL`
- Keep `languages` as raw text for now
- Preserve `source_file` and `ingested_at` for traceability

## 5. Notes

- The `raw` layer is intended for traceable ingestion, not for business-ready analytics
- The `staging` layer is the first controlled transformation step
- Additional business logic and downstream modeling will be implemented later in the `warehouse` layer