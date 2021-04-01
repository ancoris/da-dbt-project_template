{{
    config(
        materialized    = 'view',
        schema          = 'raw_freshdesk'
    )
}}
select
        s.id,
        s.survey_id,
        s._fivetran_deleted,
        s._fivetran_synced,
        s.accepted_ratings,
        s.label,

        -- meta
        {{meta_process_time()}}                      as meta_delivery_time,
        {{meta_process_time()}}                      as meta_process_time
from {{ source('freshdesk', 'survey_question') }} s
