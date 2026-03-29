{{ config(materialized='view') }}

with relation_candidates as (
    select
        c.raw_row_id,
        c.load_batch_id,
        c.deliverable_id,
        'replaces'::text as relation_type,
        c.replaces_raw as relation_values
    from {{ ref('stg_iso_deliverables_clean') }} c

    union all

    select
        c.raw_row_id,
        c.load_batch_id,
        c.deliverable_id,
        'replaced_by'::text as relation_type,
        c.replaced_by_raw as relation_values
    from {{ ref('stg_iso_deliverables_clean') }} c
)

select
    rc.raw_row_id,
    rc.load_batch_id,
    rc.deliverable_id,
    rel.token::bigint as related_deliverable_id,
    rc.relation_type
from relation_candidates rc
cross join lateral regexp_split_to_table(
    regexp_replace(
        regexp_replace(
            regexp_replace(coalesce(rc.relation_values, ''), '^\s*\[|\]\s*$', '', 'g'),
            '''',
            '',
            'g'
        ),
        '"',
        '',
        'g'
    ),
    '\s*,\s*'
) as rel(token)
where rc.relation_values is not null
  and rel.token ~ '^\d+$'