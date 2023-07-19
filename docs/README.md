# MicroETL - Data Transformation Library and Pipeline Framework

## Introduction

I hate data transformation. For me, it's one of the most boring and tedious activities I have to do quite often. I hate it so much that I decided to create a library and framework to help me reduce the amount of time I spend doing it and to have automation as much as possible for this activity (and with reusable templates).

MicroETL is that data transformation library and pipeline-framework. It can interact with multiple Databases' backend and WEB APIs. It's designed to be used in a variety of scenarios, from simple data transformations to complex data pipelines. It is also designed to be used in a variety of environments, from local environments (included just a laptop) to cloud environments.

MicroETL is written in Python and is available as a Python package. It is also available as a Docker image, which can be used to run MicroETL pipelines in a containerized environment.

## Who is this thing for?

MicroETL is for who needs to do data transformation and doesn't like to do it. If you enjoy writing code that transforms data then move along, this is not the droid you're looking for :)

But, if you need a way to transform data that is reusable, easy to maintain and easy to extend, then MicroETL might be for you!

## Features

* **Data Transformation Library** - MicroETL provides a library of data transformation functions that can be used to transform data in a variety of ways. These functions can be used to build data pipelines, or they can be used independently to transform data in other applications.

* **Data Pipeline Framework** - MicroETL provides a framework for building data pipelines. A data pipeline is a sequence of data transformation steps that are executed in a specific order. MicroETL provides a framework for building data pipelines that can be executed in a variety of environments, including local environments, cloud environments, and containerized environments.

* **Data Transformation Language** - MicroETL provides a data transformation language (DSL) and SQL templating that can both be used to define reusable data transformation logic. The data transformation language is a simple, declarative language that can be used to define data transformation pipelines in a variety of ways. Both the SQL templating and the DSL can also incorporate Python functions to extend their capabilities.

* **Data Transformation Pipeline** - MicroETL provides a data transformation pipeline that can be used to execute a set of steps to process data. Each step in the pipeline is represented by a YAML job description (and also this YAML can use templating via JINJA, so it's reusable). The data transformation pipeline is a simple, declarative pipeline that can be used to execute data transformation pipelines in a variety of ways. Each Job steps YAML has an input and an output data source (that can also be just files or APIs or Databases). The output of a step can be used as the input of the next step in the pipeline.

## Installation

MicroETL is available as a Python package. It can be installed using pip:

```bash
pip install microetl
```

MicroETL is also available as a Docker image. It can be pulled from Docker Hub:

```bash
docker pull zfpsystems/microetl
```

## Usage

MicroETL can be used in a variety of ways. It can be used as a library, as a framework, or as a language.

### Library

To use MicroETL as a library, import the `microetl` module:

```python
import microetl
```

Then check [here](MicroETL_Library.md) for more details on using it as a library with your own code.

### Framework

MicroETL can be used as a framework. It provides a framework for building data pipelines. A data pipeline is a sequence of data transformation steps that are executed in a specific order. MicroETL provides a framework for building data pipelines that can be executed in a variety of environments, including local environments, cloud environments, and containerized environments.

To use MicroETL as a framework, all you need to do is either create (or reuse by copying or linking one of the available templates) a YAML file that defines the pipeline, in the jobs directory (you can copy or link multiple YAML files in there, they will be processed in order) and then run the `microetl` command.

In case you do not wish to use the default `jobs` directory, you can specify a different directory using the `-j` or `--jobs` command line option.

```bash
microetl -j /path/to/jobs/directory
```

For more details on how to create a YAML file that defines the pipeline, check [here](jobs_configuration_via_YAML.md).

Each job file will be executed in series, so to create a pipeline, all you need to do is create multiple job files and call them in such a way that will order them in the way you want them to be executed.

For instance:

```bash
Pipeline001_step001.yaml
Pipeline001_step002.yaml
Pipeline001_step003.yaml
```

You get the idea.

#### Data Transformation Language

There are cases when you wish to have a fine grain control over the execution of the pipeline. For example, when you want to transform complex data structures, or when you want to use a different data source for each step in the pipeline. In these cases, you can also create SQL templates and even use a DSL (Domain Specific Language) to define exactly how you wish data to be transformed.

This means that you can use MicroETL to transform one JSON Object (or file) into a completely different one, or a dataset into another. You can also use it to transform a dataset into a SQL database, or a SQL database into a dataset.

Check [here](DataTransformation_via_DSL.md) for more details on using it as a DSL.

CHeck [here](DataTransformation_via_SQL.md) for more details on using it as a SQL template.

There is also an "in-development" work going on to add transformation from GraphQL to SQL (which is not yet ready for prime time).
