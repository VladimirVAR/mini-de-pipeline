CREATE TABLE IF NOT EXISTS warehouse.fact_deliverable_snapshot (
    raw_row_id                   BIGINT PRIMARY KEY REFERENCES staging.iso_deliverables_clean(raw_row_id) ON DELETE CASCADE,
    load_batch_id                BIGINT      NOT NULL REFERENCES raw.load_batches(load_batch_id) ON DELETE CASCADE,
    source_row_num               INTEGER     NOT NULL,
    deliverable_id               BIGINT,
    deliverable_type             TEXT,
    supplement_type              TEXT,
    reference                    TEXT,
    title_en                     TEXT,
    title_fr                     TEXT,
    publication_date             DATE,
    publication_year             INTEGER,
    edition                      INTEGER,
    owner_committee              TEXT,
    current_stage_raw            TEXT,
    current_stage_code           TEXT,
    stage_family_code            TEXT,
    stage_subcode                TEXT,
    is_published                 BOOLEAN     NOT NULL,
    is_withdrawn                 BOOLEAN     NOT NULL,
    has_title_en                 BOOLEAN     NOT NULL,
    has_title_fr                 BOOLEAN     NOT NULL,
    pages_en                     INTEGER,
    scope_en_html                TEXT,
    scope_en_text                TEXT,
    source_file                  TEXT        NOT NULL,
    ingested_at                  TIMESTAMPTZ NOT NULL,
    warehouse_loaded_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_wh_fact_deliverable_snapshot_batch_row UNIQUE (load_batch_id, source_row_num)
);

CREATE INDEX IF NOT EXISTS idx_wh_fact_deliverable_snapshot_batch
    ON warehouse.fact_deliverable_snapshot (load_batch_id);

CREATE INDEX IF NOT EXISTS idx_wh_fact_deliverable_snapshot_batch_deliverable
    ON warehouse.fact_deliverable_snapshot (load_batch_id, deliverable_id);
