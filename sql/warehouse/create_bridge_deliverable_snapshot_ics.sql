CREATE TABLE IF NOT EXISTS warehouse.bridge_deliverable_snapshot_ics (
    raw_row_id          BIGINT      NOT NULL REFERENCES warehouse.fact_deliverable_snapshot(raw_row_id) ON DELETE CASCADE,
    load_batch_id       BIGINT      NOT NULL REFERENCES raw.load_batches(load_batch_id) ON DELETE CASCADE,
    deliverable_id      BIGINT,
    ics_code            TEXT        NOT NULL,
    warehouse_loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (raw_row_id, ics_code)
);

CREATE INDEX IF NOT EXISTS idx_wh_bridge_ics_batch
    ON warehouse.bridge_deliverable_snapshot_ics (load_batch_id);
