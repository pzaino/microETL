# Usage

---

MicroETL jobs (or pipeline steps) can be defined using YAML files. A YAML file is a file that contains a sequence of steps that are executed in a specific order. Each step is defined by a set of parameters that are used to configure the step. The parameters are defined using a YAML syntax.

MicroETL yaml jobs support the concept of templating via JINJA, which means you can create YAML templates that can be used to transform data. This is a very powerful feature, as it allows you to use YAML to transform data in a variety of ways.

Here is an example:

```yaml
---
# YAML template file for microETL to convert any data source to any data destination using SQL
description: Convert any data source to any data destination using SQL
version: 0.0.1
datasources:
  source:
    db_type: {{ source_db_type }}
    host: {{ source_host }}
    port: {{ source_port }}
    user: {{ source_user }}
    password: {{ source_password }}
    database: {{ source_database }}
  destination:
    db_type: {{ destination_db_type }}
    host: {{ destination_host }}
    port: {{ destination_port }}
    user: {{ destination_user }}
    password: {{ destination_password }}
    database: {{ destination_database }}
schemas:
  source: 
    name: {{ source_schema_name }}
    description: "Source schema"
    type: {{ source_schema_type }}
  destination:
    name: {{ destination_schema_name }}
    description: "Destination schema"
    type: {{ destination_schema_type }}
actions:
  source:
    name: DataFetchSQLQuery
    description: "Retrieve data from source using an SQL query template and parameters"
    type: SQL_to_DataFrame
    template: {{ source_sql_template }}
    parameters: 
      - name: parameters_file
        value: {{ source_parameters_file }}
        type: file
  destination:
    name: DataStoreSQLQuery
    description: "Convert source query results and store them in destination using an SQL query template and parameters"
    type: SQL
    template: {{ destination_sql_template }}
    parameters:
      - name: parameters_df
        value: null
        type: dataframe
author: me
published: true
```
