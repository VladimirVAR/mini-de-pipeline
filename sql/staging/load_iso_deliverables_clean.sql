TRUNCATE TABLE staging.iso_deliverables_clean;

INSERT INTO staging.iso_deliverables_clean (
    id,
    deliverable_type,
    supplement_type,
    reference,
    title_en,
    title_fr,
    publication_date,
    edition,
    ics_code,
    owner_committee,
    current_stage,
    replaces,
    replaced_by,
    languages_raw,
    pages_en,
    scope_en,
    source_file,
    ingested_at
)
SELECT
    CASE
        WHEN id ~ '^\d+$' THEN id::BIGINT
        ELSE NULL
    END AS id,
    deliverable_type,
    supplement_type,
    reference,
    title_en,
    title_fr,
    CASE
        WHEN publication_date ~ '^\d{4}-\d{2}-\d{2}$'
             AND to_char(to_date(publication_date, 'YYYY-MM-DD'), 'YYYY-MM-DD') = publication_date
        THEN to_date(publication_date, 'YYYY-MM-DD')
        ELSE NULL
    END AS publication_date,
    edition,
    ics_code,
    owner_committee,
    current_stage,
    replaces,
    replaced_by,
    languages AS languages_raw,
    CASE
        WHEN pages_en ~ '^\d+$' THEN pages_en::INTEGER
        ELSE NULL
    END AS pages_en,
    scope_en,
    source_file,
    ingested_at
FROM raw.iso_deliverables;