{{
    config(
        materialized    = 'scd2_history',
        schema          = 'pl_reference',
        natural_key_col = 'survey_natural_key',
        cluster_by      = 'survey_surrogate_key, survey_natural_key, meta_start_time'
    )
}}
select
        e.survey_surrogate_key,
        e.survey_natural_key,

        -- attributes
        -- CHECK - REMOVE THE NATURAL KEY E.G. E.ID
        e.id,
        e._fivetran_deleted,

        e._fivetran_synced_time,
        e._fivetran_synced_date,


        e.created_at_time,
        e.created_at_date,

        e.title,

        e.updated_at_time,
        e.updated_at_date,


        -- meta
        e.meta_process_time,
        e.meta_delivery_time,
        e.meta_scd_action,
        e.meta_start_time,
from {{ ref('dim_survey_events') }} e
