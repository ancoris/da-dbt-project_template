{{
    config(
        materialized='materialization_none',
        enabled = False
    )
}}

-- This can be created elsewhere, kept within dbt for simplicity
-- All inserts handled by cloud function

create table if not exists `{{ this.database }}`.`dw_utils`.`cloud_function_error` (cloud_function STRING, error_time STRING, error STRING)
