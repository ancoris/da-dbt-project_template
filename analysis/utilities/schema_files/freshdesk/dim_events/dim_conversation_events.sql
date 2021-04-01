{{
    config(
        materialized    = 'scd2_events',
        schema          = 'pl_reference',
        ignore_deletes  = 'N',
        mode            = 'full',
        check_cols      = [
        'id',
        'ticket_id',

        '_fivetran_synced_time',
        '_fivetran_synced_date',

        'body',
        'body_text',
        'contact_id',

        'created_at_time',
        'created_at_date',

        'from_email',
        'incoming',
        'private',
        'source',
        'support_email',

        'updated_at_time',
        'updated_at_date',

        ],
        natural_key_col = 'conversation_natural_key',
        modified_time   = 'meta_process_time',
        created_time    = 'create_date',
        cluster_by      = 'conversation_natural_key',
        partition_by    = {'field': 'date(meta_process_time)',
                            'data_type':'date'}
    )
}}

select c.id          as conversation_natural_key,  /* CHECK THIS */
        c.id,
        c.ticket_id,

        c._fivetran_synced_time,
        c._fivetran_synced_date,

        c.body,
        c.body_text,
        c.contact_id,

        c.created_at_time,
        c.created_at_date,

        c.from_email,
        c.incoming,
        c.private,
        c.source,
        c.support_email,

        c.updated_at_time,
        c.updated_at_date,

        cast('2000-1-1' as date)                                    as create_date,

        -- meta
        c.meta_source,
        c.meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time
from {{ ref('conversation_clean') }} c
where c.meta_is_valid = 1