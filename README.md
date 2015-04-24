# vertica-python

[![PyPI version](https://badge.fury.io/py/vertica-python.png)](http://badge.fury.io/py/vertica-python)

0.4.0 breaks some of the older query interfaces (row_handler callback, and connection.query).
It replaces the row_handler callback with an iterate() method. Please see examples below

vertica-python is a native Python adapter for the Vertica (http://www.vertica.com) database.

vertica-python is currently in alpha stage; it has been tested for functionality, but does not have a test suite. Please use with caution, and feel free to submit issues and/or pull requests.

vertica-python has been tested with Vertica 6.1.2/7.0.0+ and Python 2.6/2.7.


## Installation

If you're using pip >= 1.4 and you don't already have pytz installed:

    pip install --pre pytz

To install vertica-python with pip:

    pip install vertica-python

To install vertica-python with pip (with optional namedparams dependencies):

    # see 'Using named parameters' section below
    pip install 'vertica-python[namedparams]'

Source code for vertica-python can be found at:

    http://github.com/uber/vertica-python

## Usage
**Stream** results:

```python
from vertica_python import connect

connection = connect({
    'host': '127.0.0.1',
    'port': 5433,
    'user': 'some_user',
    'password': 'some_password',
    'database': 'a_database'

    })

cur = connection.cursor()
cur.execute("SELECT * FROM a_table LIMIT 2")
for row in cur.iterate():
    print(row)
# {'id': 1, 'value': 'something'}
# {'id': 2, 'value': 'something_else'}

connection.close()
```
Streaming is recommended if you want to further process each row, save the results in a non-list/dict format (e.g. Pandas DataFrame), or save the results in a file.

**In-memory** results as list:

```python
cur = connection.cursor()
cur.execute("SELECT * FROM a_table LIMIT 2")
cur.fetchall()
# [ [1, 'something'], [2, 'something_else'] ]
connection.close()
```


**In-memory** results as dictionary:

```python
cur = connection.cursor('dict')
cur.execute("SELECT * FROM a_table LIMIT 2")
cur.fetchall()
# [ {'id': 1, 'value': 'something'}, {'id': 2, 'value': 'something_else'} ]
connection.close()
```


**Using named parameters** :

```python
# Using named parameter bindings requires psycopg2>=2.5.1 which is not includes with the base vertica_python requirements.

cur = connection.cursor()
cur.execute("SELECT * FROM a_table WHERE a = :propA b = :propB", {'propA': 1, 'propB': 'stringValue'})
cur.fetchall()
# [ [1, 'something'], [2, 'something_else'] ]

connection.close()
```


**Copy** :

```python
cur = connection.cursor()
cur.copy("COPY test_copy (id, name) from stdin DELIMITER ',' ",  "1,foo\n2,bar")

# input stream copy is todo

```


## License

MIT License, please see `LICENSE` for details.


## Acknowledgements

Many thanks go to the contributors to the Ruby Vertica gem (https://github.com/sprsquish/vertica), since they did all of the wrestling with Vertica's protocol and have kept the gem updated. They are:

 * [Matt Bauer](http://github.com/mattbauer)
 * [Jeff Smick](http://github.com/sprsquish)
 * [Willem van Bergen](http://github.com/wvanbergen)
 * [Camilo Lopez](http://github.com/camilo)
