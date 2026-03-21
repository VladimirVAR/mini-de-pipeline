DELETE FROM staging.iso_deliverables_clean
WHERE load_batch_id = %(load_batch_id)s;

INSERT INTO staging.iso_deliverables_clean (
    raw_row_id,
    load_batch_id,
    source_row_num,
    deliverable_id,
    deliverable_type,
    supplement_type,
    reference,
    title_en,
    title_fr,
    publication_date,
    publication_year,
    edition,
    owner_committee,
    current_stage_raw,
    current_stage_code,
    stage_family_code,
    stage_subcode,
    is_published,
    is_withdrawn,
    has_title_en,
    has_title_fr,
    ics_code_raw,
    replaces_raw,
    replaced_by_raw,
    languages_raw,
    pages_en,
    scope_en_html,
    scope_en_text,
    source_file,
    ingested_at
)
WITH base AS (
    SELECT
        r.raw_row_id,
        r.load_batch_id,
        r.source_row_num,
        CASE
            WHEN r.id ~ '^\d+$' THEN r.id::BIGINT
            ELSE NULL
        END AS deliverable_id,
        NULLIF(BTRIM(r.deliverable_type), '') AS deliverable_type,
        NULLIF(BTRIM(r.supplement_type), '') AS supplement_type,
        NULLIF(BTRIM(r.reference), '') AS reference,
        NULLIF(BTRIM(r.title_en), '') AS title_en,
        NULLIF(BTRIM(r.title_fr), '') AS title_fr,
        CASE
            WHEN r.publication_date ~ '^\d{4}-\d{2}-\d{2}$'
             AND TO_CHAR(TO_DATE(r.publication_date, 'YYYY-MM-DD'), 'YYYY-MM-DD') = r.publication_date
            THEN TO_DATE(r.publication_date, 'YYYY-MM-DD')
            ELSE NULL
        END AS publication_date,
        CASE
            WHEN r.edition ~ '^\d+$' THEN r.edition::INTEGER
            ELSE NULL
        END AS edition,
        NULLIF(BTRIM(r.owner_committee), '') AS owner_committee,
        NULLIF(BTRIM(r.current_stage), '') AS current_stage_raw,
        NULLIF(REGEXP_REPLACE(COALESCE(r.current_stage, ''), '\D', '', 'g'), '') AS current_stage_code,
        r.ics_code      AS ics_code_raw,
        r.replaces      AS replaces_raw,
        r.replaced_by   AS replaced_by_raw,
        r.languages     AS languages_raw,
        CASE
            WHEN r.pages_en ~ '^\d+$' THEN r.pages_en::INTEGER
            ELSE NULL
        END AS pages_en,
        r.scope_en AS scope_en_html,
        NULLIF(
            BTRIM(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(COALESCE(r.scope_en, ''), '<[^>]+>', ' ', 'g'),
                    '\s+',
                    ' ',
                    'g'
                )
            ),
            ''
        ) AS scope_en_text,
        r.source_file,
        r.ingested_at
    FROM raw.iso_deliverables r
    WHERE r.load_batch_id = %(load_batch_id)s
)
SELECT
    b.raw_row_id,
    b.load_batch_id,
    b.source_row_num,
    b.deliverable_id,
    b.deliverable_type,
    b.supplement_type,
    b.reference,
    b.title_en,
    b.title_fr,
    b.publication_date,
    EXTRACT(YEAR FROM b.publication_date)::INTEGER AS publication_year,
    b.edition,
    b.owner_committee,
    b.current_stage_raw,
    b.current_stage_code,
    CASE
        WHEN b.current_stage_code IS NOT NULL AND LENGTH(b.current_stage_code) >= 2
        THEN SUBSTRING(b.current_stage_code FROM 1 FOR 2)
        ELSE NULL
    END AS stage_family_code,
    CASE
        WHEN b.current_stage_code IS NOT NULL AND LENGTH(b.current_stage_code) >= 4
        THEN SUBSTRING(b.current_stage_code FROM 3 FOR 2)
        ELSE NULL
    END AS stage_subcode,
    (b.current_stage_code = '6060') AS is_published,
    (LEFT(COALESCE(b.current_stage_code, ''), 2) = '95') AS is_withdrawn,
    (b.title_en IS NOT NULL) AS has_title_en,
    (b.title_fr IS NOT NULL) AS has_title_fr,
    b.ics_code_raw,
    b.replaces_raw,
    b.replaced_by_raw,
    b.languages_raw,
    b.pages_en,
    b.scope_en_html,
    b.scope_en_text,
    b.source_file,
    b.ingested_at
FROM base b;
