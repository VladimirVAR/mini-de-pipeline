DROP VIEW IF EXISTS warehouse.vw_deliverables_current CASCADE;

DROP TABLE IF EXISTS warehouse.factless_deliverable_relation_snapshot CASCADE;
DROP TABLE IF EXISTS warehouse.bridge_deliverable_snapshot_ics CASCADE;
DROP TABLE IF EXISTS warehouse.bridge_deliverable_snapshot_languages CASCADE;
DROP TABLE IF EXISTS warehouse.fact_deliverable_snapshot CASCADE;
DROP TABLE IF EXISTS warehouse.iso_deliverables_core CASCADE;

DROP TABLE IF EXISTS staging.iso_deliverable_relations CASCADE;
DROP TABLE IF EXISTS staging.iso_deliverable_ics CASCADE;
DROP TABLE IF EXISTS staging.iso_deliverable_languages CASCADE;
DROP TABLE IF EXISTS staging.iso_deliverables_clean CASCADE;

DROP TABLE IF EXISTS raw.iso_deliverables CASCADE;
DROP TABLE IF EXISTS raw.load_batches CASCADE;