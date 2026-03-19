TRUNCATE TABLE warehouse.iso_deliverables_core;

INSERT INTO warehouse.iso_deliverables_core (
    deliverable_id,
    reference,
    deliverable_type,
    supplement_type,
    title_en,
    title_fr,
    publication_date,
    publication_year,
    edition,
    ics_code,
    owner_committee,
    current_stage,
    pages_en,
    has_title_en,
    has_title_fr,
    source_file,
    ingested_at
)
SELECT
    id AS deliverable_id,
    reference,
    deliverable_type,
    supplement_type,
    title_en,
    title_fr,
    publication_date,
    EXTRACT(YEAR FROM publication_date)::INTEGER AS publication_year,
    edition,
    ics_code,
    owner_committee,
    current_stage,
    pages_en,
    (title_en IS NOT NULL AND btrim(title_en) <> '') AS has_title_en,
    (title_fr IS NOT NULL AND btrim(title_fr) <> '') AS has_title_fr,
    source_file,
    ingested_at
FROM staging.iso_deliverables_clean;