# Usage

---

microETL supports JSON to JSON transformation via bender, which requires the use of a mapping and the source `dict` as arguments. microETL will raise an Exception if anything bad happens during the transformation phase.

The mapping itself is a dict whose values are benders, i.e. objects that represent the transformations to be done to the source dict. Ex:

```python
import json

from microetl import K, S

json_schema = '' # For this example we don't need a schema, however if you provide one it will be used to validate the output

MAPPING = {
    'fullName': (S('customer', 'first_name') +
                 K(' ') +
                 S('customer', 'last_name')),
    'city': S('address', 'city'),
}

data_object = {
    'customer': {
        'first_name': 'Inigo',
        'last_name': 'Montoya',
        'Age': 24,
    },
    'address': {
        'city': 'Sicily',
        'country': 'Florin',
    },
}

result = microetl.transform_data_json_to_json(data_object, MAPPING, json_schema) # json_schema is optional

print(json.dumps(result))
```

```json
{"city": "Sicily", "fullName": "Inigo Montoya"}
```

## Benders

### Selectors

#### K

`K()` is a selector for constant values:
It takes any value as a parameter and always returns that value regardless of the input.

#### S

`S()` is a selector for accessing keys and indices: It takes a variable number of keys / indices and returns the corresponding value on the source dict:

```python
from microetl import S

MAPPING = {'val': S('a', 'deeply', 'nested', 0, 'value')}

ret = microetl.transform_data_json_to_json({'a': {'deeply': {'nested': [{'value': 42}]}}}, MAPPING, '')

assert ret == {'val': 42}
```

If any of keys may not exist, `S()` can be "annotated" by calling the `.optional(default)` method, which returns an instance of `OptionalS`.
`.optional()` takes a single parameter which is passed as the `default` value of `OptionalS`; it defaults to `None`.

#### OptionalS

`OptionalS` is like `S()` but does not raise errors when any of the keys is not found. Instead, it returns `None` or the `default` value that is passed on its construction.

```python
from microetl import OptionalS

source = {'does': {'exist': 23}}

MAPPING_1 = {'val': OptionalS('does', 'not', 'exist')}

ret =  microetl.transform_data_json_to_json(source, MAPPING_1, '')

assert ret == {'val': None}

MAPPING_2 = {'val': OptionalS('does', 'not', 'exist', default=27)}

ret = microetl.transform_data_json_to_json(source, MAPPING_2)

assert ret == {'val': 27}
```

For readability and reusability, prefer using `S().optional()` instead.

#### F

`F()` lifts a python callable into a Bender, so it can be called at bending time.
It is useful for performing more complex operations for which actual python code is necessary.

The extra optional args and kwargs are passed to the function at
bending time after the given value.

```python
from microetl import F, S

MAPPING = {
    'total_number_of_keys': F(len),
    'number_of_str_keys': F(lambda source: len([k for k in source.keys()
                                                if isinstance(k, str)])),
    'price_truncated': S('price_as_str') >> F(float) >> F(int),
}

ret = microetl.transform_data_json_to_json({'price_as_str': '42.2', 'k1': 'v', 1: 'a'}, MAPPING, '')

assert ret == {'price_truncated': 42,
               'total_number_of_keys': 3,
               'number_of_str_keys': 2}
```

If the function can't take certain values, you can protect it by calling the `.protect()` method.

```python
import math
from microetl import F, S

MAPPING_1 = {'sqrt': S('val') >> F(math.sqrt).protect()}

assert microetl.transform_data_json_to_json({'val': 4}, MAPPING_1, '') == {'sqrt': 2}
assert microetl.transform_data_json_to_json({'val': None}, MAPPING_1, '') == {'sqrt': None}

MAPPING_2 = {'sqrt': S('val') >> F(math.sqrt).protect(-1)}

assert microetl.transform_data_json_to_json({'val': -1}, MAPPING_2, '') == {'sqrt': -1}
```

### Operators

MicroETL implements most of python's binary operators.

#### Arithmetic

For the arithmetic `+`, `-`, `*`, `/`,
the behavior is to apply the operator to the bended values of each operand.

```python
from microetl import K, S

a = S('a')
b = S('b')
MAPPING = {'add': a + b, 'sub': a - b, 'mul': a * b, 'div': a / b}

ret = microetl.transform_data_json_to_json({'a': 10, 'b': 5}, MAPPING, '')

assert ret == {'add': 15, 'sub': 5, 'mul': 50, 'div': 2}

ret = microetl.transform_data_json_to_json({'first_name': 'John', 'last_name': 'Doe'},
                                           {'full_name': S('first_name') + K(' ') + S('last_name')},
                                           '')

assert ret == {'full_name': 'John Doe'}
```

#### Bitwise

The bitwise operators are not yet implemented, except for the lshift (`<<`) and rshift (`>>`).
See "Composition" below.

#### List ops

There are 4 benders for working with lists, inspired by the common functional programming operations.

##### Reduce

Similar to Python's `reduce()`.
Reduces an iterable into a single value by repeatedly applying the given
function to the elements.
The function must accept two parameters: the first is the accumulator (the
value returned from the last call), which defaults to the first element of
the iterable (it must be nonempty); the second is the next value from the
iterable.

```python
from microetl import Reduce, S

MAPPING = {'sum': S('ints') >> Reduce(lambda acc, i: acc + i)}

ret = microetl.transform_data_json_to_json({'ints': [1, 4, 7, 9]}, MAPPING, '')

assert ret == {'sum': 21}
```

##### Filter

Similar to Python's `filter()`.
Builds a new list with the elements of the iterable for which the given
function returns True.

```python
from microetl import Filter, S

MAPPING = {'even': S('ints') >> Filter(lambda i: i % 2 == 0)}

ret = microetl.transform_data_json_to_json({'ints': range(5)}, MAPPING, '')

assert ret == {'even': [0, 2, 4]}
```

##### Forall

Similar to Python's `map()`.
Builds a new list by applying the given function to each element of the
iterable.

```python
from microetl import Forall, S

MAPPING = {'doubles': S('ints') >> Forall(lambda i: i * 2)}

ret = microetl.transform_data_json_to_json({'ints': range(5)}, MAPPING, '')

assert ret == {'doubles': [0, 2, 4, 6, 8]}
```

For the common case of applying a JSONBender mapping to each element of a list,
the `.bend()` *class method* is provided, which returns a `ForallBend` instance
. `.bend()` takes the mapping and the context (optional) which are then passed
to `ForallBend`.

##### ForallBend

Bends each element of the list with given mapping and context.

If no context is passed, it "inherits" at bend-time the context passed to the outer `bend()` call.

```python
from microetl import S, ForallBend

MAPPING = {'list_of_bs': S('list_of_as') >> ForallBend({'b': S('a')})}

source = {'list_of_as': [{'a': 23}, {'a': 27}]}

ret = microetl.transform_data_json_to_json(source, MAPPING, '')

assert ret == {'list_of_bs': [{'b': 23}, {'b': 27}]}
```

##### FlatForall

Similar to Forall, but the given function must return an iterable for each
element of the iterable, which are than "flattened" into a single
list.

```python
from microetl import S, FlatForall

MAPPING = {'doubles_triples': S('ints') >> FlatForall(lambda x: [x * 2, x * 3])}

source = {'ints': [2, 15, 50]}

ret = microetl.transform_data_json_to_json(source, MAPPING, '')

assert ret == {'doubles_triples': [4, 6, 30, 45, 100, 150]}
```

#### Control Flow

Sometimes what bender to use must be decided at bending time,
so JSONBender provides 3 control flow structures:

##### Alternation

Take any number of benders, and return the value of the first one that
doesn't raise a LookupError (KeyError, IndexError etc.).

If all benders raise LookupError, re-raise the last raised exception.

```python
from microetl import S, Alternation

b = Alternation(S(1), S(0), S('key1'))

b(['a', 'b'])  #  -> 'b'
b(['a'])  #  -> 'a'
try:
    b([])  #  -> TypeError
except TypeError:
    pass

try:
    b({})  #  -> KeyError
except KeyError:
    pass

b({'key1': 23})  # -> 23
```

##### If

Takes a condition bender, and two benders (both default to K(None)).
If the condition bender evaluates to true, return the value of the first
bender. If it evaluates to false, return the value of the second bender.

```python
from microetl import K, S, If

if_ = If(S('country') == K('China'), S('first_name'), S('last_name'))

if_({'country': 'China',
     'first_name': 'Li',
     'last_name': 'Na'})  # ->  'Li'

if_({'country': 'Brazil',
     'first_name': 'Gustavo',
     'last_name': 'Kuerten'})  # -> 'Kuerten'
```

##### Switch

Take a key bender, a 'case' container of benders and a default bender
(optional).

The value returned by the key bender is used to get a bender from the
case container, which then returns the result.

If the key is not in the case container, the default is used.

If it's unavailable, raise the original LookupError.

```python
from microetl import K, S, Switch

b = Switch(S('service'),
           {'twitter': S('handle'),
            'mastodon': S('handle') + K('@') + S('server')},
           default=S('email'))

b({'service': 'twitter', 'handle': 'etandel'})  #  -> 'etandel'
b({'service': 'mastodon', 'handle': 'etandel',
   'server': 'mastodon.social'})  #  -> 'etandel@mastodon.social'
b({'service': 'facebook',
   'email': 'email@whatever.com'})  #  -> 'email@whatever.com'
```

#### String ops

MicroETL currently provides only one string-related bender.

##### Format

Return a formatted string just like `str.format()`.
Where the values to be formatted are given by benders as positional or
named parameters.

It uses the same syntax as `str.format()`

```python
from microetl import Format, S

MAPPING = {'formatted': Format('{} {} {last}',
                               S('first'),
                               S('second'),
                               last=S('last'))}

source = {'first': 'Edsger', 'second': 'W.', 'last': 'Dijkstra'}

ret = microetl.transform_data_json_to_json(source, MAPPING, '')

assert ret == {'formatted': 'Edsger W. Dijkstra'}
```

### Composition

All MicroETL JSON benders can be composed with other benders using `<<` and `>>`
to make them receive previously bended values.

```python
from microetl import F, S, Forall

MAPPING = {
    'name': S('name'),
    'pythonista': S('prog_langs') >> Forall(str.lower) >> F(lambda ls: 'python' in ls),
}
source = {
    'name': 'Mary',
    'prog_langs': ['C', 'Python', 'Lua'],
}

ret = microetl.transform_data_json_to_json(source, MAPPING, '')

assert ret == {'name': 'Mary', 'pythonista': True}
```
