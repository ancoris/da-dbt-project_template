select string_agg(a, "\n")
from
(
  select concat('s.',column_name, ',') a
  from `cognolink.INFORMATION_SCHEMA.COLUMNS`
  where table_name="client"
  order by ordinal_position
)
