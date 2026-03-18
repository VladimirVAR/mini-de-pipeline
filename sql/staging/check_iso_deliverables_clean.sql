SELECT COUNT(*) AS raw_row_count
FROM raw.iso_deliverables;

SELECT COUNT(*) AS staging_row_count
FROM staging.iso_deliverables_clean;

SELECT COUNT(*) AS invalid_id_count
FROM raw.iso_deliverables
WHERE id IS NULL
   OR id !~ '^\d+$';

SELECT COUNT(*) AS invalid_publication_date_count
FROM raw.iso_deliverables
WHERE publication_date IS NULL
   OR NOT (
       publication_date ~ '^\d{4}-\d{2}-\d{2}$'
       AND to_char(to_date(publication_date, 'YYYY-MM-DD'), 'YYYY-MM-DD') = publication_date
   );

SELECT COUNT(*) AS invalid_pages_en_count
FROM raw.iso_deliverables
WHERE pages_en IS NULL
   OR pages_en !~ '^\d+$';

SELECT id, publication_date
FROM raw.iso_deliverables
WHERE publication_date IS NULL
   OR NOT (
       publication_date ~ '^\d{4}-\d{2}-\d{2}$'
       AND to_char(to_date(publication_date, 'YYYY-MM-DD'), 'YYYY-MM-DD') = publication_date
   )
LIMIT 20;

SELECT id, pages_en
FROM raw.iso_deliverables
WHERE pages_en IS NULL
   OR pages_en !~ '^\d+$'
LIMIT 20;