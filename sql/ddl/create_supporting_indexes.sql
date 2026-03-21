-- V2 addon: performance / access-path indexes
-- Run after table DDL is created.
-- Safe to re-run.

-- raw
CREATE INDEX IF NOT EXISTS idx_load_batches_status_batch_desc
    ON raw.load_batches (status, load_batch_id DESC);

CREATE INDEX IF NOT EXISTS idx_load_batches_source_file
    ON raw.load_batches (source_file);

CREATE INDEX IF NOT EXISTS idx_raw_iso_deliverables_source_file
    ON raw.iso_deliverables (source_file);

-- staging main
CREATE INDEX IF NOT EXISTS idx_stg_iso_deliverables_clean_batch_stage
    ON staging.iso_deliverables_clean (load_batch_id, current_stage_code);

CREATE INDEX IF NOT EXISTS idx_stg_iso_deliverables_clean_batch_committee
    ON staging.iso_deliverables_clean (load_batch_id, owner_committee);

CREATE INDEX IF NOT EXISTS idx_stg_iso_deliverables_clean_deliverable_batch_desc
    ON staging.iso_deliverables_clean (deliverable_id, load_batch_id DESC);

-- staging child tables
CREATE INDEX IF NOT EXISTS idx_stg_iso_deliverable_languages_batch_raw_row
    ON staging.iso_deliverable_languages (load_batch_id, raw_row_id);

CREATE INDEX IF NOT EXISTS idx_stg_iso_deliverable_languages_batch_deliverable
    ON staging.iso_deliverable_languages (load_batch_id, deliverable_id);

CREATE INDEX IF NOT EXISTS idx_stg_iso_deliverable_languages_batch_language
    ON staging.iso_deliverable_languages (load_batch_id, language_code);

CREATE INDEX IF NOT EXISTS idx_stg_iso_deliverable_ics_batch_raw_row
    ON staging.iso_deliverable_ics (load_batch_id, raw_row_id);

CREATE INDEX IF NOT EXISTS idx_stg_iso_deliverable_ics_batch_deliverable
    ON staging.iso_deliverable_ics (load_batch_id, deliverable_id);

CREATE INDEX IF NOT EXISTS idx_stg_iso_deliverable_ics_batch_ics_code
    ON staging.iso_deliverable_ics (load_batch_id, ics_code);

CREATE INDEX IF NOT EXISTS idx_stg_iso_deliverable_relations_batch_raw_row
    ON staging.iso_deliverable_relations (load_batch_id, raw_row_id);

CREATE INDEX IF NOT EXISTS idx_stg_iso_deliverable_relations_batch_deliverable
    ON staging.iso_deliverable_relations (load_batch_id, deliverable_id);

CREATE INDEX IF NOT EXISTS idx_stg_iso_deliverable_relations_batch_related
    ON staging.iso_deliverable_relations (load_batch_id, related_deliverable_id);

CREATE INDEX IF NOT EXISTS idx_stg_iso_deliverable_relations_batch_relation_type
    ON staging.iso_deliverable_relations (load_batch_id, relation_type);

-- warehouse fact
CREATE INDEX IF NOT EXISTS idx_wh_fact_deliverable_snapshot_batch_stage
    ON warehouse.fact_deliverable_snapshot (load_batch_id, current_stage_code);

CREATE INDEX IF NOT EXISTS idx_wh_fact_deliverable_snapshot_batch_committee
    ON warehouse.fact_deliverable_snapshot (load_batch_id, owner_committee);

CREATE INDEX IF NOT EXISTS idx_wh_fact_deliverable_snapshot_batch_type
    ON warehouse.fact_deliverable_snapshot (load_batch_id, deliverable_type);

CREATE INDEX IF NOT EXISTS idx_wh_fact_deliverable_snapshot_deliverable_batch_desc
    ON warehouse.fact_deliverable_snapshot (deliverable_id, load_batch_id DESC);

-- warehouse child tables
CREATE INDEX IF NOT EXISTS idx_wh_bridge_deliverable_snapshot_languages_batch_deliverable
    ON warehouse.bridge_deliverable_snapshot_languages (load_batch_id, deliverable_id);

CREATE INDEX IF NOT EXISTS idx_wh_bridge_deliverable_snapshot_languages_batch_language
    ON warehouse.bridge_deliverable_snapshot_languages (load_batch_id, language_code);

CREATE INDEX IF NOT EXISTS idx_wh_bridge_deliverable_snapshot_ics_batch_deliverable
    ON warehouse.bridge_deliverable_snapshot_ics (load_batch_id, deliverable_id);

CREATE INDEX IF NOT EXISTS idx_wh_bridge_deliverable_snapshot_ics_batch_ics_code
    ON warehouse.bridge_deliverable_snapshot_ics (load_batch_id, ics_code);

CREATE INDEX IF NOT EXISTS idx_wh_factless_deliverable_relation_snapshot_batch_deliverable
    ON warehouse.factless_deliverable_relation_snapshot (load_batch_id, deliverable_id);

CREATE INDEX IF NOT EXISTS idx_wh_factless_deliverable_relation_snapshot_batch_related
    ON warehouse.factless_deliverable_relation_snapshot (load_batch_id, related_deliverable_id);

CREATE INDEX IF NOT EXISTS idx_wh_factless_deliverable_relation_snapshot_batch_relation_type
    ON warehouse.factless_deliverable_relation_snapshot (load_batch_id, relation_type);
