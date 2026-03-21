CREATE TABLE IF NOT EXISTS staging.iso_deliverable_languages (
    raw_row_id        BIGINT      NOT NULL REFERENCES staging.iso_deliverables_clean(raw_row_id) ON DELETE CASCADE,
    load_batch_id     BIGINT      NOT NULL REFERENCES raw.load_batches(load_batch_id) ON DELETE CASCADE,
    deliverable_id    BIGINT,
    language_code     TEXT        NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (raw_row_id, language_code)
);

CREATE INDEX IF NOT EXISTS idx_stg_iso_deliverable_languages_batch
    ON staging.iso_deliverable_languages (load_batch_id);
