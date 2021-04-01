{{
    config(
        materialized    = 'scd2_history',
        schema          = 'pl_reference',
        natural_key_col = 'satisfaction_rating_value_natural_key',
        cluster_by      = 'satisfaction_rating_value_surrogate_key, satisfaction_rating_value_natural_key, meta_start_time'
    )
}}
select
        e.satisfaction_rating_value_surrogate_key,
        e.satisfaction_rating_value_natural_key,

        -- attributes
        -- CHECK - REMOVE THE NATURAL KEY E.G. E.ID
        e.satisfaction_rating_id,
        e.survey_question_id,

        e._fivetran_synced_time,
        e._fivetran_synced_date,

        e.value,

        -- meta
        e.meta_process_time,
        e.meta_delivery_time,
        e.meta_scd_action,
        e.meta_start_time,
from {{ ref('dim_satisfaction_rating_value_events') }} e
