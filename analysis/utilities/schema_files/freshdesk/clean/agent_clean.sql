{{
    config(
        materialized    = 'view',
        schema          ='clean_freshdesk'
    )
}}

select
        r.id                                                        as id,
        r._fivetran_deleted                                         as _fivetran_deleted,

        r._fivetran_synced                                          as _fivetran_synced_time,
        cast(r._fivetran_synced as date)                            as _fivetran_synced_date,

        r.available                                                 as available,

        r.available_since                                           as available_since_time,
        cast(r.available_since as date)                             as available_since_date,

        r.contact_active                                            as contact_active,

        r.contact_created_at                                        as contact_created_at_time,
        cast(r.contact_created_at as date)                          as contact_created_at_date,

        r.contact_email                                             as contact_email,
        r.contact_job_title                                         as contact_job_title,
        r.contact_language                                          as contact_language,

        r.contact_last_login_at                                     as contact_last_login_at_time,
        cast(r.contact_last_login_at as date)                       as contact_last_login_at_date,

        r.contact_mobile                                            as contact_mobile,
        r.contact_name                                              as contact_name,
        r.contact_phone                                             as contact_phone,
        r.contact_time_zone                                         as contact_time_zone,

        r.contact_updated_at                                        as contact_updated_at_time,
        cast(r.contact_updated_at as date)                          as contact_updated_at_date,


        r.created_at                                                as created_at_time,
        cast(r.created_at as date)                                  as created_at_date,

        r.occasional                                                as occasional,
        r.signature                                                 as signature,
        r.ticket_scope                                              as ticket_scope,

        r.updated_at                                                as updated_at_time,
        cast(r.updated_at as date)                                  as updated_at_date,


        -- meta
        r.meta_delivery_time                                        as meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time,
        'freshdesk'                                                 as meta_source,
        1                                                           as meta_is_valid
from {{ ref('agent_raw') }} r
