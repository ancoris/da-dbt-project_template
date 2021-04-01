{{
    config(
        materialized    = 'scd2_events',
        schema          = 'pl_reference',
        ignore_deletes  = 'N',
        mode            = 'full',
        check_cols      = [
        'id',
        '_fivetran_deleted',

        '_fivetran_synced_time',
        '_fivetran_synced_date',

        'active',

        'created_at_time',
        'created_at_date',

        'group_id',
        'name',
        'primary_role',
        'product_id',
        'reply_email',
        'to_email',

        'updated_at_time',
        'updated_at_date',

        ],
        natural_key_col = 'email_config_natural_key',
        modified_time   = 'meta_process_time',
        created_time    = 'create_date',
        cluster_by      = 'email_config_natural_key',
        partition_by    = {'field': 'date(meta_process_time)',
                            'data_type':'date'}
    )
}}

select c.id          as email_config_natural_key,  /* CHECK THIS */
        c.id,
        c._fivetran_deleted,

        c._fivetran_synced_time,
        c._fivetran_synced_date,

        c.active,

        c.created_at_time,
        c.created_at_date,

        c.group_id,
        c.name,
        c.primary_role,
        c.product_id,
        c.reply_email,
        c.to_email,

        c.updated_at_time,
        c.updated_at_date,

        cast('2000-1-1' as date)                                    as create_date,

        -- meta
        c.meta_source,
        c.meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time
from {{ ref('email_config_clean') }} c
where c.meta_is_valid = 1