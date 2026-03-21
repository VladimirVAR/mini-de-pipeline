DELETE FROM staging.iso_deliverable_relations
WHERE load_batch_id = %(load_batch_id)s;

INSERT INTO staging.iso_deliverable_relations (
    raw_row_id,
    load_batch_id,
    deliverable_id,
    related_deliverable_id,
    relation_type
)
WITH relation_candidates AS (
    SELECT
        c.raw_row_id,
        c.load_batch_id,
        c.deliverable_id,
        'replaces'::TEXT AS relation_type,
        c.replaces_raw AS relation_values
    FROM staging.iso_deliverables_clean c
    WHERE c.load_batch_id = %(load_batch_id)s

    UNION ALL

    SELECT
        c.raw_row_id,
        c.load_batch_id,
        c.deliverable_id,
        'replaced_by'::TEXT AS relation_type,
        c.replaced_by_raw AS relation_values
    FROM staging.iso_deliverables_clean c
    WHERE c.load_batch_id = %(load_batch_id)s
)
SELECT
    rc.raw_row_id,
    rc.load_batch_id,
    rc.deliverable_id,
    rel.token::BIGINT AS related_deliverable_id,
    rc.relation_type
FROM relation_candidates rc
CROSS JOIN LATERAL REGEXP_SPLIT_TO_TABLE(
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(COALESCE(rc.relation_values, ''), '^\s*\[|\]\s*$', '', 'g'),
            '''',
            '',
            'g'
        ),
        '"',
        '',
        'g'
    ),
    '\s*,\s*'
) AS rel(token)
WHERE rc.relation_values IS NOT NULL
  AND rel.token ~ '^\d+$';
