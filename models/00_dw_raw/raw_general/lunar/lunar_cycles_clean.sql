{{
    config(
        materialized='view',
        schema='raw_general_clean'
    )
}}
select  full_moon_date,
        is_total_eclipse,
        is_partial_eclipse,
        region,
        meta_delivery_time,
        meta_process_time
from    {{ref('lunar_cycles_archive')}}
where meta_process_time = {{meta_process_time() }}
