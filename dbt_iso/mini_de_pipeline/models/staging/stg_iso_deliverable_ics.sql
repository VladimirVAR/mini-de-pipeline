{{ config(materialized='view') }}

select
    c.raw_row_id,
    c.load_batch_id,
    c.deliverable_id,
    btrim(ics.token) as ics_code
from {{ ref('stg_iso_deliverables_clean') }} c
cross join lateral regexp_split_to_table(
    regexp_replace(
        regexp_replace(
            regexp_replace(coalesce(c.ics_code_raw, ''), '^\s*\[|\]\s*$', '', 'g'),
            '''',
            '',
            'g'
        ),
        '"',
        '',
        'g'
    ),
    '\s*,\s*'
) as ics(token)
where c.ics_code_raw is not null
  and nullif(btrim(ics.token), '') is not null