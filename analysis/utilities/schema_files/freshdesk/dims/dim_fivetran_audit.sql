{{
    config(
        materialized    = 'scd2_history',
        schema          = 'pl_reference',
        natural_key_col = 'fivetran_audit_natural_key',
        cluster_by      = 'fivetran_audit_surrogate_key, fivetran_audit_natural_key, meta_start_time'
    )
}}
select
        e.fivetran_audit_surrogate_key,
        e.fivetran_audit_natural_key,

        -- attributes
        -- CHECK - REMOVE THE NATURAL KEY E.G. E.ID
        e.id,

        e._fivetran_synced_time,
        e._fivetran_synced_date,


        e.done_time,
        e.done_date,

        e.message,

        e.progress_time,
        e.progress_date,

        e.rows_updated_or_inserted,
        e.schema,

        e.start_time,
        e.start_date,

        e.status,
        e.table,
        e.update_id,

        e.update_started_time,
        e.update_started_date,


        -- meta
        e.meta_process_time,
        e.meta_delivery_time,
        e.meta_scd_action,
        e.meta_start_time,
from {{ ref('dim_fivetran_audit_events') }} e
