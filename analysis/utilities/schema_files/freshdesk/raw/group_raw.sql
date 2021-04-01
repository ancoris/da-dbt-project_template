{{
    config(
        materialized    = 'view',
        schema          = 'raw_freshdesk'
    )
}}
select
        s.id,
        s._fivetran_deleted,
        s._fivetran_synced,
        s.auto_ticket_assign,
        s.business_hour_id,
        s.created_at,
        s.description,
        s.escalate_to,
        s.name,
        s.unassigned_for,
        s.updated_at,

        -- meta
        {{meta_process_time()}}                      as meta_delivery_time,
        {{meta_process_time()}}                      as meta_process_time
from {{ source('freshdesk', 'group') }} s
