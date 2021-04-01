{{
    config(
        materialized    = 'view',
        schema          = 'raw_freshdesk'
    )
}}
select
        s.agent_id,
        s.group_id,
        s._fivetran_synced,

        -- meta
        {{meta_process_time()}}                      as meta_delivery_time,
        {{meta_process_time()}}                      as meta_process_time
from {{ source('freshdesk', 'agent_group') }} s
