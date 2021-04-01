{{
    config(
        materialized    = 'scd2_history',
        schema          = 'pl_reference',
        natural_key_col = 'survey_question_natural_key',
        cluster_by      = 'survey_question_surrogate_key, survey_question_natural_key, meta_start_time'
    )
}}
select
        e.survey_question_surrogate_key,
        e.survey_question_natural_key,

        -- attributes
        -- CHECK - REMOVE THE NATURAL KEY E.G. E.ID
        e.id,
        e.survey_id,
        e._fivetran_deleted,

        e._fivetran_synced_time,
        e._fivetran_synced_date,

        e.accepted_ratings,
        e.label,

        -- meta
        e.meta_process_time,
        e.meta_delivery_time,
        e.meta_scd_action,
        e.meta_start_time,
from {{ ref('dim_survey_question_events') }} e
