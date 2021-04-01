{{
    config(
        materialized    = 'scd2_events',
        schema          = 'pl_reference',
        ignore_deletes  = 'N',
        mode            = 'full',
        check_cols      = [
        'id',

        '_fivetran_synced_time',
        '_fivetran_synced_date',

        'association_type',
        'company_id',

        'created_at_time',
        'created_at_date',

        'deleted',
        'description',
        'description_text',

        'due_by_time',
        'due_by_date',

        'email_config_id',

        'fr_due_by_time',
        'fr_due_by_date',

        'fr_escalated',
        'group_id',
        'is_escalated',
        'priority',
        'product_id',
        'requester_id',
        'responder_id',
        'source',
        'spam',

        'stats_closed_at_time',
        'stats_closed_at_date',


        'stats_first_responded_at_time',
        'stats_first_responded_at_date',


        'stats_resolved_at_time',
        'stats_resolved_at_date',

        'status',
        'subject',
        'type',

        'updated_at_time',
        'updated_at_date',

        ],
        natural_key_col = 'ticket_natural_key',
        modified_time   = 'meta_process_time',
        created_time    = 'create_date',
        cluster_by      = 'ticket_natural_key',
        partition_by    = {'field': 'date(meta_process_time)',
                            'data_type':'date'}
    )
}}

select c.id          as ticket_natural_key,  /* CHECK THIS */
        c.id,

        c._fivetran_synced_time,
        c._fivetran_synced_date,

        c.association_type,
        c.company_id,

        c.created_at_time,
        c.created_at_date,

        c.deleted,
        c.description,
        c.description_text,

        c.due_by_time,
        c.due_by_date,

        c.email_config_id,

        c.fr_due_by_time,
        c.fr_due_by_date,

        c.fr_escalated,
        c.group_id,
        c.is_escalated,
        c.priority,
        c.product_id,
        c.requester_id,
        c.responder_id,
        c.source,
        c.spam,

        c.stats_closed_at_time,
        c.stats_closed_at_date,


        c.stats_first_responded_at_time,
        c.stats_first_responded_at_date,


        c.stats_resolved_at_time,
        c.stats_resolved_at_date,

        c.status,
        c.subject,
        c.type,

        c.updated_at_time,
        c.updated_at_date,

        cast('2000-1-1' as date)                                    as create_date,

        -- meta
        c.meta_source,
        c.meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time
from {{ ref('ticket_clean') }} c
where c.meta_is_valid = 1