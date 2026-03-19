CREATE TABLE IF NOT EXISTS warehouse.iso_deliverables_core (
    deliverable_id BIGINT,
    reference TEXT,
    deliverable_type TEXT,
    supplement_type TEXT,
    title_en TEXT,
    title_fr TEXT,
    publication_date DATE,
    publication_year INTEGER,
    edition TEXT,
    ics_code TEXT,
    owner_committee TEXT,
    current_stage TEXT,
    pages_en INTEGER,
    has_title_en BOOLEAN,
    has_title_fr BOOLEAN,
    source_file TEXT,
    ingested_at TIMESTAMP
);