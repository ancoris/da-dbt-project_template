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

        'auto_ticket_assign',
        'business_hour_id',

        'created_at_time',
        'created_at_date',

        'description',
        'escalate_to',
        'name',
        'unassigned_for',

        'updated_at_time',
        'updated_at_date',

        ],
        natural_key_col = 'group_natural_key',
        modified_time   = 'meta_process_time',
        created_time    = 'create_date',
        cluster_by      = 'group_natural_key',
        partition_by    = {'field': 'date(meta_process_time)',
                            'data_type':'date'}
    )
}}

select c.id          as group_natural_key,  /* CHECK THIS */
        c.id,
        c._fivetran_deleted,

        c._fivetran_synced_time,
        c._fivetran_synced_date,

        c.auto_ticket_assign,
        c.business_hour_id,

        c.created_at_time,
        c.created_at_date,

        c.description,
        c.escalate_to,
        c.name,
        c.unassigned_for,

        c.updated_at_time,
        c.updated_at_date,

        cast('2000-1-1' as date)                                    as create_date,

        -- meta
        c.meta_source,
        c.meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time
from {{ ref('group_clean') }} c
where c.meta_is_valid = 1