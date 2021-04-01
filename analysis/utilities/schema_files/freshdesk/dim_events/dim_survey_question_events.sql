{{
    config(
        materialized    = 'scd2_events',
        schema          = 'pl_reference',
        ignore_deletes  = 'N',
        mode            = 'full',
        check_cols      = [
        'id',
        'survey_id',
        '_fivetran_deleted',

        '_fivetran_synced_time',
        '_fivetran_synced_date',

        'accepted_ratings',
        'label',
        ],
        natural_key_col = 'survey_question_natural_key',
        modified_time   = 'meta_process_time',
        created_time    = 'create_date',
        cluster_by      = 'survey_question_natural_key',
        partition_by    = {'field': 'date(meta_process_time)',
                            'data_type':'date'}
    )
}}

select c.id          as survey_question_natural_key,  /* CHECK THIS */
        c.id,
        c.survey_id,
        c._fivetran_deleted,

        c._fivetran_synced_time,
        c._fivetran_synced_date,

        c.accepted_ratings,
        c.label,
        cast('2000-1-1' as date)                                    as create_date,

        -- meta
        c.meta_source,
        c.meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time
from {{ ref('survey_question_clean') }} c
where c.meta_is_valid = 1