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
        s.agent_id,
        s.billable,
        s.company_id,
        s.created_at,
        s.executed_at,
        s.note,
        s.start_time,
        s.ticket_id,
        s.time_spent,
        s.timer_running,
        s.updated_at,

        -- meta
        {{meta_process_time()}}                      as meta_delivery_time,
        {{meta_process_time()}}                      as meta_process_time
from {{ source('freshdesk', 'time_entries') }} s
