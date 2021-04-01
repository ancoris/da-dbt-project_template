{{
    config(
        materialized    = 'view',
        schema          = 'raw_freshdesk'
    )
}}
select
        s.id,
        s._fivetran_synced,
        s.association_type,
        s.company_id,
        s.created_at,
        s.deleted,
        s.description,
        s.description_text,
        s.due_by,
        s.email_config_id,
        s.fr_due_by,
        s.fr_escalated,
        s.group_id,
        s.is_escalated,
        s.priority,
        s.product_id,
        s.requester_id,
        s.responder_id,
        s.source,
        s.spam,
        s.stats_closed_at,
        s.stats_first_responded_at,
        s.stats_resolved_at,
        s.status,
        s.subject,
        s.type,
        s.updated_at,

        -- meta
        {{meta_process_time()}}                      as meta_delivery_time,
        {{meta_process_time()}}                      as meta_process_time
from {{ source('freshdesk', 'ticket') }} s
