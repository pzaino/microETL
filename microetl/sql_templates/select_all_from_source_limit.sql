SELECT
    *
FROM
    {{ source_table_name }}
LIMIT
    {{ source_table_query_limit }};
