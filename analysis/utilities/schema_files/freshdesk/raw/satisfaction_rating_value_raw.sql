{{
    config(
        materialized    = 'view',
        schema          = 'raw_freshdesk'
    )
}}
select
        s.satisfaction_rating_id,
        s.survey_question_id,
        s._fivetran_synced,
        s.value,

        -- meta
        {{meta_process_time()}}                      as meta_delivery_time,
        {{meta_process_time()}}                      as meta_process_time
from {{ source('freshdesk', 'satisfaction_rating_value') }} s
