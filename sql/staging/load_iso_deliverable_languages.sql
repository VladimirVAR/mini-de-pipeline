DELETE FROM staging.iso_deliverable_languages
WHERE load_batch_id = %(load_batch_id)s;

INSERT INTO staging.iso_deliverable_languages (
    raw_row_id,
    load_batch_id,
    deliverable_id,
    language_code
)
SELECT
    c.raw_row_id,
    c.load_batch_id,
    c.deliverable_id,
    LOWER(BTRIM(lang.token)) AS language_code
FROM staging.iso_deliverables_clean c
CROSS JOIN LATERAL REGEXP_SPLIT_TO_TABLE(
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(COALESCE(c.languages_raw, ''), '^\s*\[|\]\s*$', '', 'g'),
            '''',
            '',
            'g'
        ),
        '"',
        '',
        'g'
    ),
    '\s*,\s*'
) AS lang(token)
WHERE c.load_batch_id = %(load_batch_id)s
  AND c.languages_raw IS NOT NULL
  AND NULLIF(BTRIM(lang.token), '') IS NOT NULL;
