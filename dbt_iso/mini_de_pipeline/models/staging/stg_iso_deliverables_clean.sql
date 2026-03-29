{{ config(materialized='view') }}

with base as (
    select
        r.raw_row_id,
        r.load_batch_id,
        r.source_row_num,
        case
            when r.id ~ '^\d+$' then r.id::bigint
            else null
        end as deliverable_id,
        nullif(btrim(r.deliverable_type), '') as deliverable_type,
        nullif(btrim(r.supplement_type), '') as supplement_type,
        nullif(btrim(r.reference), '') as reference,
        nullif(btrim(r.title_en), '') as title_en,
        nullif(btrim(r.title_fr), '') as title_fr,
        case
            when r.publication_date ~ '^\d{4}-\d{2}-\d{2}$'
             and to_char(to_date(r.publication_date, 'YYYY-MM-DD'), 'YYYY-MM-DD') = r.publication_date
            then to_date(r.publication_date, 'YYYY-MM-DD')
            else null
        end as publication_date,
        case
            when r.edition ~ '^\d+$' then r.edition::integer
            else null
        end as edition,
        nullif(btrim(r.owner_committee), '') as owner_committee,
        nullif(btrim(r.current_stage), '') as current_stage_raw,
        nullif(regexp_replace(coalesce(r.current_stage, ''), '\D', '', 'g'), '') as current_stage_code,
        r.ics_code as ics_code_raw,
        r.replaces as replaces_raw,
        r.replaced_by as replaced_by_raw,
        r.languages as languages_raw,
        case
            when r.pages_en ~ '^\d+$' then r.pages_en::integer
            else null
        end as pages_en,
        r.scope_en as scope_en_html,
        nullif(
            btrim(
                regexp_replace(
                    regexp_replace(coalesce(r.scope_en, ''), '<[^>]+>', ' ', 'g'),
                    '\s+',
                    ' ',
                    'g'
                )
            ),
            ''
        ) as scope_en_text,
        r.source_file,
        r.ingested_at
    from {{ source('raw', 'iso_deliverables') }} r
)

select
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
    extract(year from b.publication_date)::integer as publication_year,
    b.edition,
    b.owner_committee,
    b.current_stage_raw,
    b.current_stage_code,
    case
        when b.current_stage_code is not null and length(b.current_stage_code) >= 2
        then substring(b.current_stage_code from 1 for 2)
        else null
    end as stage_family_code,
    case
        when b.current_stage_code is not null and length(b.current_stage_code) >= 4
        then substring(b.current_stage_code from 3 for 2)
        else null
    end as stage_subcode,
    (b.current_stage_code = '6060') as is_published,
    (left(coalesce(b.current_stage_code, ''), 2) = '95') as is_withdrawn,
    (b.title_en is not null) as has_title_en,
    (b.title_fr is not null) as has_title_fr,
    b.ics_code_raw,
    b.replaces_raw,
    b.replaced_by_raw,
    b.languages_raw,
    b.pages_en,
    b.scope_en_html,
    b.scope_en_text,
    b.source_file,
    b.ingested_at
from base b