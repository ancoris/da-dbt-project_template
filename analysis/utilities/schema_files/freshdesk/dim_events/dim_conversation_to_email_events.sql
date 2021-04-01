{{
    config(
        materialized    = 'scd2_events',
        schema          = 'pl_reference',
        ignore_deletes  = 'N',
        mode            = 'full',
        check_cols      = [
        'conversation_id',
        'email',

        '_fivetran_synced_time',
        '_fivetran_synced_date',

        ],
        natural_key_col = 'conversation_to_email_natural_key',
        modified_time   = 'meta_process_time',
        created_time    = 'create_date',
        cluster_by      = 'conversation_to_email_natural_key',
        partition_by    = {'field': 'date(meta_process_time)',
                            'data_type':'date'}
    )
}}

select c.conversation_id          as conversation_to_email_natural_key,  /* CHECK THIS */
        c.conversation_id,
        c.email,

        c._fivetran_synced_time,
        c._fivetran_synced_date,

        cast('2000-1-1' as date)                                    as create_date,

        -- meta
        c.meta_source,
        c.meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time
from {{ ref('conversation_to_email_clean') }} c
where c.meta_is_valid = 1