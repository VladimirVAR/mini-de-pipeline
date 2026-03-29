{{ config(materialized='view') }}

select
    c.raw_row_id,
    c.load_batch_id,
    c.deliverable_id,
    lower(btrim(lang.token)) as language_code
from {{ ref('stg_iso_deliverables_clean') }} c
cross join lateral regexp_split_to_table(
    regexp_replace(
        regexp_replace(
            regexp_replace(coalesce(c.languages_raw, ''), '^\s*\[|\]\s*$', '', 'g'),
            '''',
            '',
            'g'
        ),
        '"',
        '',
        'g'
    ),
    '\s*,\s*'
) as lang(token)
where c.languages_raw is not null
  and nullif(btrim(lang.token), '') is not null