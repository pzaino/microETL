SELECT
    *
FROM
    {{ source_table_name }}
WHERE
    TO_DATE({{ source_table_date_field }}) >= TO_DATE(DATEADD(day, -1, GETDATE()));