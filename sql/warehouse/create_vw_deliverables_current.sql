CREATE OR REPLACE VIEW warehouse.vw_deliverables_current AS
WITH latest_batch AS (
    SELECT MAX(load_batch_id) AS load_batch_id
    FROM raw.load_batches
    WHERE status = 'warehouse_loaded'
)
SELECT f.*
FROM warehouse.fact_deliverable_snapshot f
JOIN latest_batch lb
  ON f.load_batch_id = lb.load_batch_id;
