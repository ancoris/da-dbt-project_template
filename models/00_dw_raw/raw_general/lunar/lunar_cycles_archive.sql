{{
    config(
        materialized='table',
        schema='raw_archive_general',
        partition_by='date(meta_process_time)'
    )
}}
select  full_moon_date,
        is_total_eclipse,
        is_partial_eclipse,
        region,
        meta_delivery_time,
        {{ meta_process_time() }}    as meta_process_time
from    {{ref('lunar_cycles_dbt_seed')}}
