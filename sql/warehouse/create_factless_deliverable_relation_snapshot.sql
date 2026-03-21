CREATE TABLE IF NOT EXISTS warehouse.factless_deliverable_relation_snapshot (
    raw_row_id             BIGINT      NOT NULL REFERENCES warehouse.fact_deliverable_snapshot(raw_row_id) ON DELETE CASCADE,
    load_batch_id          BIGINT      NOT NULL REFERENCES raw.load_batches(load_batch_id) ON DELETE CASCADE,
    deliverable_id         BIGINT,
    related_deliverable_id BIGINT      NOT NULL,
    relation_type          TEXT        NOT NULL CHECK (relation_type IN ('replaces', 'replaced_by')),
    warehouse_loaded_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (raw_row_id, relation_type, related_deliverable_id)
);

CREATE INDEX IF NOT EXISTS idx_wh_relation_snapshot_batch
    ON warehouse.factless_deliverable_relation_snapshot (load_batch_id);
