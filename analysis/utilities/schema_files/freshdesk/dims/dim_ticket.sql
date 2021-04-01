{{
    config(
        materialized    = 'scd2_history',
        schema          = 'pl_reference',
        natural_key_col = 'ticket_natural_key',
        cluster_by      = 'ticket_surrogate_key, ticket_natural_key, meta_start_time'
    )
}}
select
        e.ticket_surrogate_key,
        e.ticket_natural_key,

        -- attributes
        -- CHECK - REMOVE THE NATURAL KEY E.G. E.ID
        e.id,

        e._fivetran_synced_time,
        e._fivetran_synced_date,

        e.association_type,
        e.company_id,

        e.created_at_time,
        e.created_at_date,

        e.deleted,
        e.description,
        e.description_text,

        e.due_by_time,
        e.due_by_date,

        e.email_config_id,

        e.fr_due_by_time,
        e.fr_due_by_date,

        e.fr_escalated,
        e.group_id,
        e.is_escalated,
        e.priority,
        e.product_id,
        e.requester_id,
        e.responder_id,
        e.source,
        e.spam,

        e.stats_closed_at_time,
        e.stats_closed_at_date,


        e.stats_first_responded_at_time,
        e.stats_first_responded_at_date,


        e.stats_resolved_at_time,
        e.stats_resolved_at_date,

        e.status,
        e.subject,
        e.type,

        e.updated_at_time,
        e.updated_at_date,


        -- meta
        e.meta_process_time,
        e.meta_delivery_time,
        e.meta_scd_action,
        e.meta_start_time,
from {{ ref('dim_ticket_events') }} e
