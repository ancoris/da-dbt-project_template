select string_agg(a, "\n")
from
(
  select  concat('- name: ',column_name) a
  from    `cognolink.INFORMATION_SCHEMA.COLUMNS`
  where    table_name="client"
  order by ordinal_position
)
