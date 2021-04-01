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

        'available',

        'available_since_time',
        'available_since_date',

        'contact_active',

        'contact_created_at_time',
        'contact_created_at_date',

        'contact_email',
        'contact_job_title',
        'contact_language',

        'contact_last_login_at_time',
        'contact_last_login_at_date',

        'contact_mobile',
        'contact_name',
        'contact_phone',
        'contact_time_zone',

        'contact_updated_at_time',
        'contact_updated_at_date',


        'created_at_time',
        'created_at_date',

        'occasional',
        'signature',
        'ticket_scope',

        'updated_at_time',
        'updated_at_date',

        ],
        natural_key_col = 'agent_natural_key',
        modified_time   = 'meta_process_time',
        created_time    = 'create_date',
        cluster_by      = 'agent_natural_key',
        partition_by    = {'field': 'date(meta_process_time)',
                            'data_type':'date'}
    )
}}

select c.id          as agent_natural_key,  /* CHECK THIS */
        c.id,
        c._fivetran_deleted,

        c._fivetran_synced_time,
        c._fivetran_synced_date,

        c.available,

        c.available_since_time,
        c.available_since_date,

        c.contact_active,

        c.contact_created_at_time,
        c.contact_created_at_date,

        c.contact_email,
        c.contact_job_title,
        c.contact_language,

        c.contact_last_login_at_time,
        c.contact_last_login_at_date,

        c.contact_mobile,
        c.contact_name,
        c.contact_phone,
        c.contact_time_zone,

        c.contact_updated_at_time,
        c.contact_updated_at_date,


        c.created_at_time,
        c.created_at_date,

        c.occasional,
        c.signature,
        c.ticket_scope,

        c.updated_at_time,
        c.updated_at_date,

        cast('2000-1-1' as date)                                    as create_date,

        -- meta
        c.meta_source,
        c.meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time
from {{ ref('agent_clean') }} c
where c.meta_is_valid = 1