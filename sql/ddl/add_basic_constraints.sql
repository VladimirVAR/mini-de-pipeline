-- V2 addon: basic data-quality constraints
-- Run after table DDL is created.
-- Safe to re-run because each constraint is dropped and recreated.

-- raw.load_batches
ALTER TABLE raw.load_batches
    DROP CONSTRAINT IF EXISTS chk_load_batches_loaded_row_count_nonnegative;
ALTER TABLE raw.load_batches
    ADD CONSTRAINT chk_load_batches_loaded_row_count_nonnegative
    CHECK (loaded_row_count IS NULL OR loaded_row_count >= 0);

ALTER TABLE raw.load_batches
    DROP CONSTRAINT IF EXISTS chk_load_batches_finished_after_started;
ALTER TABLE raw.load_batches
    ADD CONSTRAINT chk_load_batches_finished_after_started
    CHECK (finished_at IS NULL OR finished_at >= started_at);

-- raw.iso_deliverables
ALTER TABLE raw.iso_deliverables
    DROP CONSTRAINT IF EXISTS chk_raw_iso_deliverables_source_row_num_positive;
ALTER TABLE raw.iso_deliverables
    ADD CONSTRAINT chk_raw_iso_deliverables_source_row_num_positive
    CHECK (source_row_num > 0);

-- staging.iso_deliverables_clean
ALTER TABLE staging.iso_deliverables_clean
    DROP CONSTRAINT IF EXISTS chk_stg_iso_deliverables_clean_source_row_num_positive;
ALTER TABLE staging.iso_deliverables_clean
    ADD CONSTRAINT chk_stg_iso_deliverables_clean_source_row_num_positive
    CHECK (source_row_num > 0);

ALTER TABLE staging.iso_deliverables_clean
    DROP CONSTRAINT IF EXISTS chk_stg_iso_deliverables_clean_publication_year_valid;
ALTER TABLE staging.iso_deliverables_clean
    ADD CONSTRAINT chk_stg_iso_deliverables_clean_publication_year_valid
    CHECK (publication_year IS NULL OR publication_year BETWEEN 1900 AND 2100);

ALTER TABLE staging.iso_deliverables_clean
    DROP CONSTRAINT IF EXISTS chk_stg_iso_deliverables_clean_publication_year_matches_date;
ALTER TABLE staging.iso_deliverables_clean
    ADD CONSTRAINT chk_stg_iso_deliverables_clean_publication_year_matches_date
    CHECK (
        publication_date IS NULL
        OR publication_year IS NULL
        OR publication_year = EXTRACT(YEAR FROM publication_date)::INTEGER
    );

ALTER TABLE staging.iso_deliverables_clean
    DROP CONSTRAINT IF EXISTS chk_stg_iso_deliverables_clean_edition_positive;
ALTER TABLE staging.iso_deliverables_clean
    ADD CONSTRAINT chk_stg_iso_deliverables_clean_edition_positive
    CHECK (edition IS NULL OR edition >= 1);

ALTER TABLE staging.iso_deliverables_clean
    DROP CONSTRAINT IF EXISTS chk_stg_iso_deliverables_clean_pages_en_nonnegative;
ALTER TABLE staging.iso_deliverables_clean
    ADD CONSTRAINT chk_stg_iso_deliverables_clean_pages_en_nonnegative
    CHECK (pages_en IS NULL OR pages_en >= 0);

ALTER TABLE staging.iso_deliverables_clean
    DROP CONSTRAINT IF EXISTS chk_stg_iso_deliverables_clean_current_stage_code_format;

ALTER TABLE staging.iso_deliverables_clean
    ADD CONSTRAINT chk_stg_iso_deliverables_clean_current_stage_code_format
    CHECK (current_stage_code IS NULL OR current_stage_code ~ '^\d{2}(\d{2})?$');

ALTER TABLE staging.iso_deliverables_clean
    DROP CONSTRAINT IF EXISTS chk_stg_iso_deliverables_clean_stage_family_code_format;
ALTER TABLE staging.iso_deliverables_clean
    ADD CONSTRAINT chk_stg_iso_deliverables_clean_stage_family_code_format
    CHECK (stage_family_code IS NULL OR stage_family_code ~ '^\d{2}$');

ALTER TABLE staging.iso_deliverables_clean
    DROP CONSTRAINT IF EXISTS chk_stg_iso_deliverables_clean_stage_subcode_format;
ALTER TABLE staging.iso_deliverables_clean
    ADD CONSTRAINT chk_stg_iso_deliverables_clean_stage_subcode_format
    CHECK (stage_subcode IS NULL OR stage_subcode ~ '^\d{2}$');

ALTER TABLE staging.iso_deliverables_clean
    DROP CONSTRAINT IF EXISTS chk_stg_iso_deliverables_clean_has_title_en_consistent;
ALTER TABLE staging.iso_deliverables_clean
    ADD CONSTRAINT chk_stg_iso_deliverables_clean_has_title_en_consistent
    CHECK (has_title_en = (title_en IS NOT NULL));

ALTER TABLE staging.iso_deliverables_clean
    DROP CONSTRAINT IF EXISTS chk_stg_iso_deliverables_clean_has_title_fr_consistent;
ALTER TABLE staging.iso_deliverables_clean
    ADD CONSTRAINT chk_stg_iso_deliverables_clean_has_title_fr_consistent
    CHECK (has_title_fr = (title_fr IS NOT NULL));

-- staging child tables
ALTER TABLE staging.iso_deliverable_languages
    DROP CONSTRAINT IF EXISTS chk_stg_iso_deliverable_languages_language_code_not_blank;
ALTER TABLE staging.iso_deliverable_languages
    ADD CONSTRAINT chk_stg_iso_deliverable_languages_language_code_not_blank
    CHECK (language_code IS NOT NULL AND BTRIM(language_code) <> '');

ALTER TABLE staging.iso_deliverable_ics
    DROP CONSTRAINT IF EXISTS chk_stg_iso_deliverable_ics_ics_code_not_blank;
ALTER TABLE staging.iso_deliverable_ics
    ADD CONSTRAINT chk_stg_iso_deliverable_ics_ics_code_not_blank
    CHECK (ics_code IS NOT NULL AND BTRIM(ics_code) <> '');

ALTER TABLE staging.iso_deliverable_relations
    DROP CONSTRAINT IF EXISTS chk_stg_iso_deliverable_relations_relation_type_valid;
ALTER TABLE staging.iso_deliverable_relations
    ADD CONSTRAINT chk_stg_iso_deliverable_relations_relation_type_valid
    CHECK (relation_type IN ('replaces', 'replaced_by'));

-- warehouse.fact_deliverable_snapshot
ALTER TABLE warehouse.fact_deliverable_snapshot
    DROP CONSTRAINT IF EXISTS chk_wh_fact_deliverable_snapshot_current_stage_code_format;

ALTER TABLE warehouse.fact_deliverable_snapshot
    ADD CONSTRAINT chk_wh_fact_deliverable_snapshot_current_stage_code_format
    CHECK (current_stage_code IS NULL OR current_stage_code ~ '^\d{2}(\d{2})?$');

ALTER TABLE warehouse.fact_deliverable_snapshot
    DROP CONSTRAINT IF EXISTS chk_wh_fact_deliverable_snapshot_source_row_num_positive;
ALTER TABLE warehouse.fact_deliverable_snapshot
    ADD CONSTRAINT chk_wh_fact_deliverable_snapshot_source_row_num_positive
    CHECK (source_row_num > 0);

ALTER TABLE warehouse.fact_deliverable_snapshot
    DROP CONSTRAINT IF EXISTS chk_wh_fact_deliverable_snapshot_publication_year_valid;
ALTER TABLE warehouse.fact_deliverable_snapshot
    ADD CONSTRAINT chk_wh_fact_deliverable_snapshot_publication_year_valid
    CHECK (publication_year IS NULL OR publication_year BETWEEN 1900 AND 2100);

ALTER TABLE warehouse.fact_deliverable_snapshot
    DROP CONSTRAINT IF EXISTS chk_wh_fact_deliverable_snapshot_publication_year_matches_date;
ALTER TABLE warehouse.fact_deliverable_snapshot
    ADD CONSTRAINT chk_wh_fact_deliverable_snapshot_publication_year_matches_date
    CHECK (
        publication_date IS NULL
        OR publication_year IS NULL
        OR publication_year = EXTRACT(YEAR FROM publication_date)::INTEGER
    );

ALTER TABLE warehouse.fact_deliverable_snapshot
    DROP CONSTRAINT IF EXISTS chk_wh_fact_deliverable_snapshot_edition_positive;
ALTER TABLE warehouse.fact_deliverable_snapshot
    ADD CONSTRAINT chk_wh_fact_deliverable_snapshot_edition_positive
    CHECK (edition IS NULL OR edition >= 1);

ALTER TABLE warehouse.fact_deliverable_snapshot
    DROP CONSTRAINT IF EXISTS chk_wh_fact_deliverable_snapshot_pages_en_nonnegative;
ALTER TABLE warehouse.fact_deliverable_snapshot
    ADD CONSTRAINT chk_wh_fact_deliverable_snapshot_pages_en_nonnegative
    CHECK (pages_en IS NULL OR pages_en >= 0);

ALTER TABLE warehouse.fact_deliverable_snapshot
    DROP CONSTRAINT IF EXISTS chk_wh_fact_deliverable_snapshot_current_stage_code_format;
ALTER TABLE warehouse.fact_deliverable_snapshot
    ADD CONSTRAINT chk_wh_fact_deliverable_snapshot_current_stage_code_format
    CHECK (current_stage_code IS NULL OR current_stage_code ~ '^\d{2}(\d{2})?$');

ALTER TABLE warehouse.fact_deliverable_snapshot
    DROP CONSTRAINT IF EXISTS chk_wh_fact_deliverable_snapshot_stage_family_code_format;
ALTER TABLE warehouse.fact_deliverable_snapshot
    ADD CONSTRAINT chk_wh_fact_deliverable_snapshot_stage_family_code_format
    CHECK (stage_family_code IS NULL OR stage_family_code ~ '^\d{2}$');

ALTER TABLE warehouse.fact_deliverable_snapshot
    DROP CONSTRAINT IF EXISTS chk_wh_fact_deliverable_snapshot_stage_subcode_format;
ALTER TABLE warehouse.fact_deliverable_snapshot
    ADD CONSTRAINT chk_wh_fact_deliverable_snapshot_stage_subcode_format
    CHECK (stage_subcode IS NULL OR stage_subcode ~ '^\d{2}$');

ALTER TABLE warehouse.fact_deliverable_snapshot
    DROP CONSTRAINT IF EXISTS chk_wh_fact_deliverable_snapshot_has_title_en_consistent;
ALTER TABLE warehouse.fact_deliverable_snapshot
    ADD CONSTRAINT chk_wh_fact_deliverable_snapshot_has_title_en_consistent
    CHECK (has_title_en = (title_en IS NOT NULL));

ALTER TABLE warehouse.fact_deliverable_snapshot
    DROP CONSTRAINT IF EXISTS chk_wh_fact_deliverable_snapshot_has_title_fr_consistent;
ALTER TABLE warehouse.fact_deliverable_snapshot
    ADD CONSTRAINT chk_wh_fact_deliverable_snapshot_has_title_fr_consistent
    CHECK (has_title_fr = (title_fr IS NOT NULL));

-- warehouse child tables
ALTER TABLE warehouse.bridge_deliverable_snapshot_languages
    DROP CONSTRAINT IF EXISTS chk_wh_bridge_deliverable_snapshot_languages_language_code_not_blank;
ALTER TABLE warehouse.bridge_deliverable_snapshot_languages
    ADD CONSTRAINT chk_wh_bridge_deliverable_snapshot_languages_language_code_not_blank
    CHECK (language_code IS NOT NULL AND BTRIM(language_code) <> '');

ALTER TABLE warehouse.bridge_deliverable_snapshot_ics
    DROP CONSTRAINT IF EXISTS chk_wh_bridge_deliverable_snapshot_ics_ics_code_not_blank;
ALTER TABLE warehouse.bridge_deliverable_snapshot_ics
    ADD CONSTRAINT chk_wh_bridge_deliverable_snapshot_ics_ics_code_not_blank
    CHECK (ics_code IS NOT NULL AND BTRIM(ics_code) <> '');

ALTER TABLE warehouse.factless_deliverable_relation_snapshot
    DROP CONSTRAINT IF EXISTS chk_wh_factless_deliverable_relation_snapshot_relation_type_valid;
ALTER TABLE warehouse.factless_deliverable_relation_snapshot
    ADD CONSTRAINT chk_wh_factless_deliverable_relation_snapshot_relation_type_valid
    CHECK (relation_type IN ('replaces', 'replaced_by'));
