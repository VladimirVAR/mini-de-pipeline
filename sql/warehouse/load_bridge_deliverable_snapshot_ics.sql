DELETE FROM warehouse.bridge_deliverable_snapshot_ics
WHERE load_batch_id = %(load_batch_id)s;

INSERT INTO warehouse.bridge_deliverable_snapshot_ics (
    raw_row_id,
    load_batch_id,
    deliverable_id,
    ics_code
)
SELECT
    raw_row_id,
    load_batch_id,
    deliverable_id,
    ics_code
FROM staging.iso_deliverable_ics
WHERE load_batch_id = %(load_batch_id)s;
