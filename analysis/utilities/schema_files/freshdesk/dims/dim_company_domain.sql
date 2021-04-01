{{
    config(
        materialized    = 'scd2_history',
        schema          = 'pl_reference',
        natural_key_col = 'company_domain_natural_key',
        cluster_by      = 'company_domain_surrogate_key, company_domain_natural_key, meta_start_time'
    )
}}
select
        e.company_domain_surrogate_key,
        e.company_domain_natural_key,

        -- attributes
        -- CHECK - REMOVE THE NATURAL KEY E.G. E.ID
        e.company_id,
        e.domain,
        e._fivetran_deleted,

        e._fivetran_synced_time,
        e._fivetran_synced_date,


        -- meta
        e.meta_process_time,
        e.meta_delivery_time,
        e.meta_scd_action,
        e.meta_start_time,
from {{ ref('dim_company_domain_events') }} e
