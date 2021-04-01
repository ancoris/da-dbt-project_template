{{
    config(
        materialized    = 'scd2_history',
        schema          = 'pl_reference',
        natural_key_col = 'ticket_tag_natural_key',
        cluster_by      = 'ticket_tag_surrogate_key, ticket_tag_natural_key, meta_start_time'
    )
}}
select
        e.ticket_tag_surrogate_key,
        e.ticket_tag_natural_key,

        -- attributes
        -- CHECK - REMOVE THE NATURAL KEY E.G. E.ID
        e.index,
        e.ticket_id,

        e._fivetran_synced_time,
        e._fivetran_synced_date,

        e.tag,

        -- meta
        e.meta_process_time,
        e.meta_delivery_time,
        e.meta_scd_action,
        e.meta_start_time,
from {{ ref('dim_ticket_tag_events') }} e
