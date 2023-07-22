# Usage

---

MicroETL jobs (or pipeline steps) can be defined using YAML files. A YAML file is a file that contains a sequence of steps that are executed in a specific order. Each step is defined by a set of parameters that are used to configure the step. The parameters are defined using a YAML syntax.

Each YAML job configuration file presents 3 distinct sections:

1) Fetch
2) Transform
3) Push

The Fetch step is used to fetch data from a data source. The Transform step is used to transform the data. The Push step is used to push the data to a data destination.

The "fetch" step is represented by the key "source" in the YAML file. The "transform" step is represented by the key "transform" in the YAML file. The "push" step is represented by the key "destination" in the YAML file.

While the "Transform" section is optional, the "Fetch" and "Push" steps are required. The "Transform" section can be divided in many steps, each one of them is a single transformation action.

Source and Destination also have the extra configuration of datasources, which are the data sources and destinations that will be used by the steps.

A simple pipeline is represented by a single YAML job file, while, a more complex pipeline is represented by a sequence of YAML files. Each YAML file represents a step in the pipeline.

In most cases a simple pipeline will suffice, but in some cases you may need to create a more complex pipeline. For example, you may need to create a pipeline that fetches data from multiple data sources, transforms the data, and then pushes the data to multiple data destinations.

## YAML Job Configuration

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
  transform:
    sequence:
      - step: "transform 001"
        type: jq
        template: {{ transform_template_jq }}
        RootElement: {{ transform_root_element }}
        template_path: {{ transform_template_path }}
      - step: "transform 002"
        type: print
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

## Templating

If needed, you can also use `!include <file>` to include other YAML files (and fragments) in your YAML template. This is useful if you want to reuse parts of your YAML template in other YAML templates.

All the allowed syntaxes for `!include` are:

### Mapping

If `Pipeline001-step001.yml` contained:

```yaml
parameters: !include include.d/parameters.yml
```

and

`include.d/parameters.yml` contained:

```yaml
- name: this_is_a_parameter
  value: this_is_a_value
- name: this_is_another_parameter
  value: this_is_another_value
```

We'll get:

```yaml
parameters:
  - name: this_is_a_parameter
    value: this_is_a_value
  - name: this_is_another_parameter
    value: this_is_another_value
```

### Sequence

If `Pipeline001-step001.yml` was:

```yaml
parameters:
  - !include include.d/par1.yml
  - !include include.d/par2.yml
```

and `include.d/par1.yml` contained:

```yaml
name: this_is_a_parameter
value: this_is_a_value
```

and `include.d/par2.yml` contained:

```yaml
name: this_is_another_parameter
value: this_is_another_value
```

We'll get:

```yaml
files:
  - name: this_is_a_parameter
    value: this_is_a_value
  - name: this_is_another_parameter
    value: this_is_another_value
```

> ℹ **Note**:
>
> File name can be either absolute (like `/usr/conf/1.5/Make.yml`) or relative (like `../../cfg/img.yml`).

### Wildcards

File name can contain shell-style wildcards. Data loaded from the file(s) found by wildcards will be set in a sequence.

That is to say, a list will be returned when including file name contains wildcards.
Length of the returned list equals number of matched files:

- when only 1 file matched, length of list will be 1
- when there are no files matched, an empty list will be returned

If `Pipeline001-step001.yml` was:

```yaml
source: 
  !include jobs/include.d/CRM_Server.yml
```

and `jobs/include.d/` contains a file named `CRM_Server.yml` with the following content:

```yaml
type: postgresql
host: localhost
port: 5432
database: CRM
user: postgres
password: postgres
```

> ℹ **Note**:
>
> Datasources, can take user and password fields empty and, if provided empty, MicroETL will try to get them from the OS environment variables. If provided directly in the job YAML file they can also be populated using jinja and, in this case, the user can decide the name of an OS environment variable to use to populate the field.

We'll get:

```yaml
source:
  type: postgresql
  host: localhost
  port: 5432
  database: CRM
  user: postgres
  password: postgres
```

> ℹ **Please note**: At this time is not possible to use jinja templating in YAML fragments that gets included in our main job YAML file.

#### Recursive

Inclusion can be set to be recursive. In this case, all files matching the pattern will be included.

> ℹ **Note**:
>
> - if `recursive` argument of `!include` [YAML] tag is `true`, the pattern `“**”` will match any files and zero or more directories and subdirectories.
> - Using the `“**”` pattern in large directory trees may consume an inordinate amount of time because of recursive search.

In order to enable `recursive` argument, we shall set it in `Mapping` or `Sequence` arguments mode:

- Arguments in `Sequence` mode:

  ```yaml
  !include [tests/jobs/include.d/**/*.yml, true]
  ```

- Arguments in `Mapping` mode:

  ```yaml
  !include {pathname: tests/jobs/include.d/**/*.yml, recursive: true}
  ```

### Non YAML files

This extending constructor can now load data from non YAML files, supported file types are:

- `json`
- `toml` (only available when [toml](https://pypi.org/project/toml/) installed)
- `ini`

### Using OS environment variables in YAML templates

You can use OS environment variables in your YAML templates. For example, if you have an environment variable called `MY_ENV_VAR`, you can use it in your YAML template like this:

```yaml
type: {{ MY_ENV_TYPE }}
```

It will be replaced with the value of the environment variable when the template is rendered.

### Using Python functions in YAML templates

You can use Python functions in your YAML templates. For example, if you have a wish to use the ETL Server date as a "live" value during a data transformation, you can use it in your YAML template like this:

```yaml
name: pyval(etl_server_date)
value: datetime.datetime.now().date()
```

What will happen is that the Python function will be called when the template is rendered, and the result will be used as the value of the parameter.

So if you wish to use it as a parameter for your SQL template for example, all you'll have to do in your SQL template is refer to it like this:

```sql
SELECT 
  {{ params.fields_list }}
FROM 
  {{ params.table_name }}
WHERE 
      {{ params.filter_field1 }} = '{{ params.etl_server_date }}'
```
