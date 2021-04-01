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
        s.active,
        s.address,
        s.company_id,
        s.created_at,
        s.description,
        s.email,
        s.job_title,
        s.language,
        s.mobile,
        s.name,
        s.phone,
        s.time_zone,
        s.twitter_id,
        s.updated_at,

        -- meta
        {{meta_process_time()}}                      as meta_delivery_time,
        {{meta_process_time()}}                      as meta_process_time
from {{ source('freshdesk', 'contact') }} s
