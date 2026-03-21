CREATE TABLE IF NOT EXISTS raw.load_batches (
    load_batch_id      BIGSERIAL PRIMARY KEY,
    source_name        TEXT        NOT NULL,
    source_file        TEXT        NOT NULL,
    file_hash_sha256   TEXT,
    load_mode          TEXT        NOT NULL CHECK (load_mode IN ('full_refresh', 'append')),
    load_method        TEXT        NOT NULL CHECK (load_method IN ('executemany', 'copy')),
    status             TEXT        NOT NULL CHECK (status IN ('started', 'raw_loaded', 'warehouse_loaded', 'failed')),
    loaded_row_count   INTEGER,
    started_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at        TIMESTAMPTZ,
    error_message      TEXT
);

CREATE INDEX IF NOT EXISTS idx_load_batches_status
    ON raw.load_batches (status);
