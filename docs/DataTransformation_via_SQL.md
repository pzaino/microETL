# Usage

---

MicroETL supports the concept of SQL templating, which means you can create SQL templates that can be used to transform data. This is a very powerful feature, as it allows you to use SQL to transform data in a variety of ways.

To use SQL templating, you need to create a SQL template file. A SQL template file is a file that contains SQL statements that can be used to transform data. The SQL statements can be used to transform data in a variety of ways. For example, you can use SQL statements to transform data into a different format, or you can use SQL statements to transform data into a different data type.

```SQL
SELECT 
  {{ params.fields_list }}
FROM 
  {{ params.table_name }}
WHERE 
      {{ params.filter_field1 }} = '{{ params.filter_value1 }}' 
  AND {{ params.filter_field2 }} = '{{ params.filter_value2 }}';
```

The SQL template file can be used to transform data in a variety of ways. For example, you can use SQL statements to transform data into a different format, or you can use SQL statements to transform data into a different data type.

You can also use conditions for your templating, for example:

```SQL
SELECT 
  {{ params.fields_list }}
FROM 
  {{ params.table_name }}
{% if params.filter_field1 %}
  WHERE 
      {{ params.filter_field1 }} = '{{ params.filter_value1 }}' 
  AND {{ params.filter_field2 }} = '{{ params.filter_value2 }}';
{% endif %}
```
