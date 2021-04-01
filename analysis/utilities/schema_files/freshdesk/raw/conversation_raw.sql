{{
    config(
        materialized    = 'view',
        schema          = 'raw_freshdesk'
    )
}}
select
        s.id,
        s.ticket_id,
        s._fivetran_synced,
        s.body,
        s.body_text,
        s.contact_id,
        s.created_at,
        s.from_email,
        s.incoming,
        s.private,
        s.source,
        s.support_email,
        s.updated_at,

        -- meta
        {{meta_process_time()}}                      as meta_delivery_time,
        {{meta_process_time()}}                      as meta_process_time
from {{ source('freshdesk', 'conversation') }} s
