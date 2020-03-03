-- replace your dataset and table name in the below query and run on bq and save results as a Google Sheet

select  concat('- name: ',column_name, '\n')
from    `bigquery-public-data.london_bicycles.INFORMATION_SCHEMA.COLUMNS`
where    table_name="cycle_hire"
order by ordinal_position

-- example output (add this in your schema.yml)
- name: rental_id

- name: duration

- name: bike_id

- name: end_date

- name: end_station_id

- name: end_station_name

- name: start_date

- name: start_station_id

- name: start_station_name

- name: end_station_logical_terminal

- name: start_station_logical_terminal

- name: end_station_priority_id
