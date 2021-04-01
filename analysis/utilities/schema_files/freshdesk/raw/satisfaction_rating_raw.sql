{{
    config(
        materialized    = 'view',
        schema          = 'raw_freshdesk'
    )
}}
select
        s.id,
        s._fivetran_synced,
        s.agent_id,
        s.contact_id,
        s.created_at,
        s.feedback,
        s.group_id,
        s.survey_id,
        s.ticket_id,
        s.updated_at,

        -- meta
        {{meta_process_time()}}                      as meta_delivery_time,
        {{meta_process_time()}}                      as meta_process_time
from {{ source('freshdesk', 'satisfaction_rating') }} s
