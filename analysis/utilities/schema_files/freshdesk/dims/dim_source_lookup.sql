{{
    config(
        materialized    = 'scd2_history',
        schema          = 'pl_reference',
        natural_key_col = 'source_lookup_natural_key',
        cluster_by      = 'source_lookup_surrogate_key, source_lookup_natural_key, meta_start_time'
    )
}}
select
        e.source_lookup_surrogate_key,
        e.source_lookup_natural_key,

        -- attributes
        -- CHECK - REMOVE THE NATURAL KEY E.G. E.ID
        e.id,
        e._fivetran_deleted,

        e._fivetran_synced_time,
        e._fivetran_synced_date,

        e.source,

        -- meta
        e.meta_process_time,
        e.meta_delivery_time,
        e.meta_scd_action,
        e.meta_start_time,
from {{ ref('dim_source_lookup_events') }} e
