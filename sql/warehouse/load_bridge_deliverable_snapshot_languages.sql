DELETE FROM warehouse.bridge_deliverable_snapshot_languages
WHERE load_batch_id = %(load_batch_id)s;

INSERT INTO warehouse.bridge_deliverable_snapshot_languages (
    raw_row_id,
    load_batch_id,
    deliverable_id,
    language_code
)
SELECT
    raw_row_id,
    load_batch_id,
    deliverable_id,
    language_code
FROM staging.iso_deliverable_languages
WHERE load_batch_id = %(load_batch_id)s;
