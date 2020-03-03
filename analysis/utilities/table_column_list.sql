-- replace your dataset and table name in the below query and run on bq and save results as a Google Sheet
select  concat(column_name, ',')
from    `bigquery-public-data.london_bicycles.INFORMATION_SCHEMA.COLUMNS`
where    table_name="cycle_hire"
order by ordinal_position

-- example output
rental_id,
duration,
bike_id,
end_date,
end_station_id,
end_station_name,
start_date,
start_station_id,
start_station_name,
end_station_logical_terminal,
start_station_logical_terminal,
end_station_priority_id,
