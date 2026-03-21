CREATE TABLE IF NOT EXISTS raw.iso_deliverables (
    raw_row_id         BIGSERIAL PRIMARY KEY,
    load_batch_id      BIGINT      NOT NULL REFERENCES raw.load_batches(load_batch_id) ON DELETE CASCADE,
    source_row_num     INTEGER     NOT NULL,
    id                 TEXT,
    deliverable_type   TEXT,
    supplement_type    TEXT,
    reference          TEXT,
    title_en           TEXT,
    title_fr           TEXT,
    publication_date   TEXT,
    edition            TEXT,
    ics_code           TEXT,
    owner_committee    TEXT,
    current_stage      TEXT,
    replaces           TEXT,
    replaced_by        TEXT,
    languages          TEXT,
    pages_en           TEXT,
    scope_en           TEXT,
    source_file        TEXT        NOT NULL,
    ingested_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_raw_iso_deliverables_batch_row UNIQUE (load_batch_id, source_row_num)
);

CREATE INDEX IF NOT EXISTS idx_raw_iso_deliverables_batch
    ON raw.iso_deliverables (load_batch_id);

CREATE INDEX IF NOT EXISTS idx_raw_iso_deliverables_batch_id
    ON raw.iso_deliverables (load_batch_id, id);
