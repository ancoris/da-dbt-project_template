dbt run --vars '{scenario: A}'

dbt run --vars '{scenario: A, replay_process_time: 2020-01-01 00:00:00 UTC}'

dbt run --vars '{scenario: A}' --models dim_date

dbt run --vars '{scenario: A}' --full-refresh

dbt test --models dim_date
