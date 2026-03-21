CREATE TABLE IF NOT EXISTS staging.iso_deliverable_relations (
    raw_row_id             BIGINT      NOT NULL REFERENCES staging.iso_deliverables_clean(raw_row_id) ON DELETE CASCADE,
    load_batch_id          BIGINT      NOT NULL REFERENCES raw.load_batches(load_batch_id) ON DELETE CASCADE,
    deliverable_id         BIGINT,
    related_deliverable_id BIGINT      NOT NULL,
    relation_type          TEXT        NOT NULL CHECK (relation_type IN ('replaces', 'replaced_by')),
    created_at             TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (raw_row_id, relation_type, related_deliverable_id)
);

CREATE INDEX IF NOT EXISTS idx_stg_iso_deliverable_relations_batch
    ON staging.iso_deliverable_relations (load_batch_id);
