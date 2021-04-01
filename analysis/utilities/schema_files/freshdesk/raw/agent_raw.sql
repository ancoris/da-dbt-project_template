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
        s.available,
        s.available_since,
        s.contact_active,
        s.contact_created_at,
        s.contact_email,
        s.contact_job_title,
        s.contact_language,
        s.contact_last_login_at,
        s.contact_mobile,
        s.contact_name,
        s.contact_phone,
        s.contact_time_zone,
        s.contact_updated_at,
        s.created_at,
        s.occasional,
        s.signature,
        s.ticket_scope,
        s.updated_at,

        -- meta
        {{meta_process_time()}}                      as meta_delivery_time,
        {{meta_process_time()}}                      as meta_process_time
from {{ source('freshdesk', 'agent') }} s
