DELETE FROM staging.iso_deliverable_ics
WHERE load_batch_id = %(load_batch_id)s;

INSERT INTO staging.iso_deliverable_ics (
    raw_row_id,
    load_batch_id,
    deliverable_id,
    ics_code
)
SELECT
    c.raw_row_id,
    c.load_batch_id,
    c.deliverable_id,
    BTRIM(ics.token) AS ics_code
FROM staging.iso_deliverables_clean c
CROSS JOIN LATERAL REGEXP_SPLIT_TO_TABLE(
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(COALESCE(c.ics_code_raw, ''), '^\s*\[|\]\s*$', '', 'g'),
            '''',
            '',
            'g'
        ),
        '"',
        '',
        'g'
    ),
    '\s*,\s*'
) AS ics(token)
WHERE c.load_batch_id = %(load_batch_id)s
  AND c.ics_code_raw IS NOT NULL
  AND NULLIF(BTRIM(ics.token), '') IS NOT NULL;
