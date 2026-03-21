-- Showcase Queries for Mini DE Pipeline (ISO Deliverables)
--
-- Purpose:
-- Demonstrate that the warehouse layer is not only loaded correctly,
-- but is also useful for analytical querying.
--
-- Recommended usage:
-- 1) Run individual queries one by one.
-- 2) Prefer latest snapshot / current view for business-facing examples.
-- 3) Use snapshot history queries to demonstrate append-mode behavior.

/* -------------------------------------------------------------------------- */
/* 1. Current-state summary: published vs withdrawn                            */
/* -------------------------------------------------------------------------- */
SELECT
    COUNT(*) AS total_deliverables,
    SUM(CASE WHEN is_published THEN 1 ELSE 0 END) AS published_count,
    SUM(CASE WHEN is_withdrawn THEN 1 ELSE 0 END) AS withdrawn_count,
    SUM(CASE WHEN NOT is_withdrawn THEN 1 ELSE 0 END) AS non_withdrawn_count
FROM warehouse.vw_deliverables_current;

/* -------------------------------------------------------------------------- */
/* 2. Distribution by deliverable type                                        */
/* -------------------------------------------------------------------------- */
SELECT
    deliverable_type,
    COUNT(*) AS deliverable_count
FROM warehouse.vw_deliverables_current
GROUP BY deliverable_type
ORDER BY deliverable_count DESC, deliverable_type;

/* -------------------------------------------------------------------------- */
/* 3. Top owner committees                                                    */
/* -------------------------------------------------------------------------- */
SELECT
    owner_committee,
    COUNT(*) AS deliverable_count
FROM warehouse.vw_deliverables_current
GROUP BY owner_committee
ORDER BY deliverable_count DESC, owner_committee
LIMIT 15;

/* -------------------------------------------------------------------------- */
/* 4. Distribution by stage family                                            */
/* -------------------------------------------------------------------------- */
SELECT
    stage_family_code,
    COUNT(*) AS deliverable_count
FROM warehouse.vw_deliverables_current
GROUP BY stage_family_code
ORDER BY stage_family_code;

/* -------------------------------------------------------------------------- */
/* 5. Language usage in the latest snapshot                                   */
/* -------------------------------------------------------------------------- */
SELECT
    language_code,
    COUNT(*) AS usage_count
FROM warehouse.bridge_deliverable_snapshot_languages
WHERE load_batch_id = (SELECT MAX(load_batch_id) FROM warehouse.fact_deliverable_snapshot)
GROUP BY language_code
ORDER BY usage_count DESC, language_code;

/* -------------------------------------------------------------------------- */
/* 6. Average number of languages per deliverable in latest snapshot          */
/* -------------------------------------------------------------------------- */
WITH language_counts AS (
    SELECT
        load_batch_id,
        deliverable_id,
        COUNT(*) AS language_count
    FROM warehouse.bridge_deliverable_snapshot_languages
    WHERE load_batch_id = (SELECT MAX(load_batch_id) FROM warehouse.fact_deliverable_snapshot)
    GROUP BY load_batch_id, deliverable_id
)
SELECT
    AVG(language_count::numeric) AS avg_languages_per_deliverable,
    MIN(language_count) AS min_languages,
    MAX(language_count) AS max_languages
FROM language_counts;

/* -------------------------------------------------------------------------- */
/* 7. Top ICS codes in the latest snapshot                                    */
/* -------------------------------------------------------------------------- */
SELECT
    ics_code,
    COUNT(*) AS usage_count
FROM warehouse.bridge_deliverable_snapshot_ics
WHERE load_batch_id = (SELECT MAX(load_batch_id) FROM warehouse.fact_deliverable_snapshot)
GROUP BY ics_code
ORDER BY usage_count DESC, ics_code
LIMIT 20;

/* -------------------------------------------------------------------------- */
/* 8. Relationship counts by relation type                                    */
/* -------------------------------------------------------------------------- */
SELECT
    relation_type,
    COUNT(*) AS relation_count
FROM warehouse.factless_deliverable_relation_snapshot
WHERE load_batch_id = (SELECT MAX(load_batch_id) FROM warehouse.fact_deliverable_snapshot)
GROUP BY relation_type
ORDER BY relation_type;

/* -------------------------------------------------------------------------- */
/* 9. Example lineage pairs with source / related references                  */
/* -------------------------------------------------------------------------- */
SELECT
    r.relation_type,
    d1.reference AS source_reference,
    d2.reference AS related_reference
FROM warehouse.factless_deliverable_relation_snapshot r
JOIN warehouse.fact_deliverable_snapshot d1
    ON d1.load_batch_id = r.load_batch_id
   AND d1.deliverable_id = r.deliverable_id
JOIN warehouse.fact_deliverable_snapshot d2
    ON d2.load_batch_id = r.load_batch_id
   AND d2.deliverable_id = r.related_deliverable_id
WHERE r.load_batch_id = (SELECT MAX(load_batch_id) FROM warehouse.fact_deliverable_snapshot)
LIMIT 25;

/* -------------------------------------------------------------------------- */
/* 10. Snapshot history by batch                                              */
/* -------------------------------------------------------------------------- */
SELECT
    load_batch_id,
    COUNT(*) AS deliverable_count
FROM warehouse.fact_deliverable_snapshot
GROUP BY load_batch_id
ORDER BY load_batch_id;

/* -------------------------------------------------------------------------- */
/* 11. Current view should point to latest batch only                         */
/* -------------------------------------------------------------------------- */
SELECT
    load_batch_id,
    COUNT(*) AS row_count
FROM warehouse.vw_deliverables_current
GROUP BY load_batch_id
ORDER BY load_batch_id;

/* -------------------------------------------------------------------------- */
/* 12. Deliverables without languages in latest snapshot                      */
/* -------------------------------------------------------------------------- */
SELECT
    COUNT(*) AS deliverables_without_languages
FROM warehouse.fact_deliverable_snapshot f
LEFT JOIN warehouse.bridge_deliverable_snapshot_languages l
    ON l.load_batch_id = f.load_batch_id
   AND l.deliverable_id = f.deliverable_id
WHERE f.load_batch_id = (SELECT MAX(load_batch_id) FROM warehouse.fact_deliverable_snapshot)
  AND l.deliverable_id IS NULL;

/* -------------------------------------------------------------------------- */
/* 13. Deliverables without ICS codes in latest snapshot                      */
/* -------------------------------------------------------------------------- */
SELECT
    COUNT(*) AS deliverables_without_ics
FROM warehouse.fact_deliverable_snapshot f
LEFT JOIN warehouse.bridge_deliverable_snapshot_ics i
    ON i.load_batch_id = f.load_batch_id
   AND i.deliverable_id = f.deliverable_id
WHERE f.load_batch_id = (SELECT MAX(load_batch_id) FROM warehouse.fact_deliverable_snapshot)
  AND i.deliverable_id IS NULL;
