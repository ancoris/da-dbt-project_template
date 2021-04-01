{{
    config(
        materialized    = 'view',
        schema          ='clean_freshdesk'
    )
}}

select
        r.email                                                     as email,
        r.ticket_id                                                 as ticket_id,

        r._fivetran_synced                                          as _fivetran_synced_time,
        cast(r._fivetran_synced as date)                            as _fivetran_synced_date,


        -- meta
        r.meta_delivery_time                                        as meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time,
        'freshdesk'                                                 as meta_source,
        1                                                           as meta_is_valid
from {{ ref('ticket_to_email_raw') }} r
