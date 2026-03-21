DELETE FROM warehouse.factless_deliverable_relation_snapshot
WHERE load_batch_id = %(load_batch_id)s;

INSERT INTO warehouse.factless_deliverable_relation_snapshot (
    raw_row_id,
    load_batch_id,
    deliverable_id,
    related_deliverable_id,
    relation_type
)
SELECT
    raw_row_id,
    load_batch_id,
    deliverable_id,
    related_deliverable_id,
    relation_type
FROM staging.iso_deliverable_relations
WHERE load_batch_id = %(load_batch_id)s;
