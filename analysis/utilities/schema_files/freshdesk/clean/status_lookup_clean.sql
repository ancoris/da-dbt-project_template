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

        r.label_for_agents                                          as label_for_agents,
        r.label_for_customer                                        as label_for_customer,

        -- meta
        r.meta_delivery_time                                        as meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time,
        'freshdesk'                                                 as meta_source,
        1                                                           as meta_is_valid
from {{ ref('status_lookup_raw') }} r
