{{
    config(
        materialized    = 'scd2_events',
        schema          = 'pl_reference',
        ignore_deletes  = 'N',
        mode            = 'full',
        check_cols      = [
        'conversation_id',
        'id',

        '_fivetran_synced_time',
        '_fivetran_synced_date',

        'attachment_url',
        'content_type',

        'created_at_time',
        'created_at_date',

        'name',
        'size',

        'updated_at_time',
        'updated_at_date',

        ],
        natural_key_col = 'conversation_attachment_natural_key',
        modified_time   = 'meta_process_time',
        created_time    = 'create_date',
        cluster_by      = 'conversation_attachment_natural_key',
        partition_by    = {'field': 'date(meta_process_time)',
                            'data_type':'date'}
    )
}}

select c.conversation_id          as conversation_attachment_natural_key,  /* CHECK THIS */
        c.conversation_id,
        c.id,

        c._fivetran_synced_time,
        c._fivetran_synced_date,

        c.attachment_url,
        c.content_type,

        c.created_at_time,
        c.created_at_date,

        c.name,
        c.size,

        c.updated_at_time,
        c.updated_at_date,

        cast('2000-1-1' as date)                                    as create_date,

        -- meta
        c.meta_source,
        c.meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time
from {{ ref('conversation_attachment_clean') }} c
where c.meta_is_valid = 1