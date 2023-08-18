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

If you're not familiar with the CDDL v1.1 license, please check [this](https://fossa.com/blog/open-source-licenses-101-cddl-common-development-distribution-license/) link for more "human readable" info.

In very short terms the CDDL v1.1 is a weak copyleft license which allows you to link my work with copyrighted source, however, if you modify my work (or improve it) you still need to release your changes under CDDL v1.1 and therefore make your changes available to everyone to benefit. That does NOT include your software that may be using my work, only the improvements you've made to my work.

Also, obviously, you cannot claim it's your work, you must always give credit to the original author (me) and all the folks that have contributed to this project.

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
