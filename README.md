# microETL

A library and framework for data transformation, ETL (Extract, Transform and Load) and data migration using multiple data sources, DSL and multiple data destinations.

## Installation

Clone this repo and then run

```bash
cd microETL
pip install -r requirements.txt
```

After that, make sure your copy of jinjaSQL is using markupsafe library. To do that:

(on the mac):

```bash
vi $(homebrew_prefix)/lib/python3.9/site-packages/jinjasql/core.py
```

(on MS Windows, using VSCode)

```powershell
code C:\Users\<your-user-name>\AppData\Local\Programs\Python\Python<ver>\Lib\site-packages\jinjasql\core.py
```

Where

* `<your-user-name>` is (obviously) your Windows username
* `<ver>` is your Python version (for example 311 for python 3.11)

And, around line 6, if you see this import:

```python
from jinja2.utils import Markup
```

comment it out and immediately after, add this:

```python
from markupsafe import Markup
```

## Usage

To learn everything about using microETL, please check the [docs](docs/README.md).

## Contributing

MicroETL is released under CDDL v1.1 license, so everyone can contribute to make it better. Before contributing, please read the [docs](docs/README.md) to make sure you fully understand all the goals and so your contributions are always in-line with the project goals, thank you.

## License

CDDL v1.1

## Acknowledgements

Given that MicroETL is mostly a "process" that uses other libraries, I would like to thank the authors of the following libraries:

* JinjaSQL
* PyYAML
* Jinja2
* MarkupSafe
* SQLAlchemy
* JSON
* Pandas
* Numpy
* JSONBender
* Snowflake client
* MySQL client
* PostgreSQL client
* SQLite client
* Neo4J client
* MongoDB client
* JSONSchema

I hope I have included them all, if I missed any, please let me know and I will add them to the list, thank you!
