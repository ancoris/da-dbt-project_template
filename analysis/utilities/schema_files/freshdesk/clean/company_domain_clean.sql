{{
    config(
        materialized    = 'view',
        schema          ='clean_freshdesk'
    )
}}

select
        r.company_id                                                as company_id,
        r.domain                                                    as domain,
        r._fivetran_deleted                                         as _fivetran_deleted,

        r._fivetran_synced                                          as _fivetran_synced_time,
        cast(r._fivetran_synced as date)                            as _fivetran_synced_date,


        -- meta
        r.meta_delivery_time                                        as meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time,
        'freshdesk'                                                 as meta_source,
        1                                                           as meta_is_valid
from {{ ref('company_domain_raw') }} r
