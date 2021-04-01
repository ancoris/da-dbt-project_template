{{
    config(
        materialized    = 'view',
        schema          = 'raw_freshdesk'
    )
}}
select
        s.id,
        s._fivetran_synced,
        s.done,
        s.message,
        s.progress,
        s.rows_updated_or_inserted,
        s.schema,
        s.start,
        s.status,
        s.table,
        s.update_id,
        s.update_started,

        -- meta
        {{meta_process_time()}}                      as meta_delivery_time,
        {{meta_process_time()}}                      as meta_process_time
from {{ source('freshdesk', 'fivetran_audit') }} s
