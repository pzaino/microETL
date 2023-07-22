# Usage

---

MicroETL supports also JQ syntax to create a transformation pipeline. The JQ syntax is a JSON query language that allows to transform JSON data. The JQ syntax is very powerful and allows to create complex transformation pipelines. The JQ syntax is described in detail in the [JQ manual](https://stedolan.github.io/jq/manual/).

You can use the JQ syntax to create a transformation pipeline within the Transformation section of the job yaml file.

You can specify a JQ template file name that will be loaded and "compiled" to transform the data as you wish.

Given that some DB will return JSON data without a root element, this can cause some issue when dealing with JQ syntax and so MicroETL allows to specify a root element name that will be used to wrap the JSON data before applying the JQ syntax.

here is how to do that:

```yaml
Transformation:
  sequence:
    - step: "jq transformation 001"
      type: jq
      template_path: pyexpr(base_path)/jobs/include.d/jq_templates
      Template: "jq_template.jq"
      RootElement: "source"
```

The step name is used for logging purposes.
The type to specify must be "jq" (it's case insensitive, so also JQ is ok).
The template_path is the path where the JQ template file is located. The path can be absolute or relative to the job yaml file. The path can contain also python expressions that will be evaluated to get the final path (in which case the python expression MUSt be enclosed in pyexpr() function).
The Template is the name of the JQ template file to use.
The RootElement is the name of the root element to use to wrap the JSON data before applying the JQ syntax.

The JQ template file must be a valid JQ syntax file. The JQ syntax is described in detail in the [JQ manual](https://stedolan.github.io/jq/manual/).

Here is an example:

```jq
{
    id: [ .source[].oid ], 
    dbms: [ .source[].datname ], 
    encoding: [ .source[].datctype + ", encoding" ]
}
```
