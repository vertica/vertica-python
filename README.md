# vertica-python

[![PyPI version](https://badge.fury.io/py/vertica-python.svg)](https://badge.fury.io/py/vertica-python)
[![License](https://img.shields.io/badge/License-Apache%202.0-orange.svg)](https://opensource.org/licenses/Apache-2.0)

:loudspeaker: 08/14/2018: *vertica-python* becomes Vertica’s first officially supported open source database client, see the blog [here](https://my.vertica.com/blog/vertica-python-becomes-verticas-first-officially-supported-open-source-database-client/).

0.6.x adds python3 support (unicode namedparams support is currently broken in python3, see issue 112)

0.5.x changes the connection method to accept kwargs instead of a dict to be more dbapi compliant.
      copy methods improved and consolidated in 0.5.1

0.4.x breaks some of the older query interfaces (row_handler callback, and connection.query).
It replaces the row_handler callback with an iterate() method. Please see examples below
If you are on 0.4.x, please upgrade to 0.4.6 as there are various bug fixes

vertica-python is a native Python adapter for the Vertica (http://www.vertica.com) database.

vertica-python is currently in beta stage; it has been tested for functionality and has a very basic test suite. Please use with caution, and feel free to submit issues and/or pull requests (after running the unit tests).

vertica-python has been tested with Vertica 6.1.2/7.0.0+ and Python 2.7/3.4.


## Installation

If you're using pip >= 1.4 and you don't already have pytz installed:

    pip install --pre pytz

If you're using pip >= 1.4 and you don't already have python-dateutil installed:

    pip install --pre python-dateutil

To install vertica-python with pip:

    pip install vertica-python

To install vertica-python with pip (with optional namedparams dependencies):

    # see 'Using named parameters' section below
    pip install 'vertica-python[namedparams]'

Source code for vertica-python can be found at:

    http://github.com/uber/vertica-python


## Run unit tests

To run the tests, you must have access to a Vertica database. Heres one way to go about it:

Download docker kitematic:
https://kitematic.com/

Spin up a vertica container (i use sumitchawla/vertica)

Edit the port number in `tests/test_commons.py` to match the container.

Install tox:
http://tox.readthedocs.io

Edit `tox.ini` envlist property to list the version(s) of python you have installed

Run all tests using tox:
```bash
tox
```

Run a test suite on specified python versions:

Run all tests under `test_case.py` on the python versions 2.7 and 3.5
```bash
tox -e py27,py35 -- vertica_python/tests/test_cases.py
```

Run an individual test on specified python versions:

Run the test `test_case` under `test_cases.py` on the python versions 2.7 and 3.5
```bash
tox -e py27,py35 -- vertica_python/tests/test_cases.py:TestCaseClass.test_case
```
## Usage


**Create connection**

```python
import vertica_python

conn_info = {'host': '127.0.0.1',
             'port': 5433,
             'user': 'some_user',
             'password': 'some_password',
             'database': 'a_database',
             # 10 minutes timeout on queries
             'read_timeout': 600,
             # default throw error on invalid UTF-8 results
             'unicode_error': 'strict',
             # SSL is disabled by default
             'ssl': False,
             'connection_timeout': 5
             # connection timeout is not enabled by default}

# simple connection, with manual close
connection = vertica_python.connect(**conn_info)
# do things
connection.close()

# using with for auto connection closing after usage
with vertica_python.connect(**conn_info) as connection:
    # do things
```

You can pass an `ssl.SSLContext` to `ssl` to customize the SSL connection options. For example,

```python
import vertica_python
import ssl

ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
ssl_context.verify_mode = ssl.CERT_REQUIRED
ssl_context.check_hostname = True
ssl_context.load_verify_locations(cafile='/path/to/ca_file.pem')

conn_info = {'host': '127.0.0.1',
             'port': 5433,
             'user': 'some_user',
             'password': 'some_password',
             'database': 'a_database',
             'ssl': ssl_context}
connection = vertica_python.connect(**conn_info)

```

See more on SSL options [here](https://docs.python.org/2/library/ssl.html).

**Stream query results**:

```python
cur = connection.cursor()
cur.execute("SELECT * FROM a_table LIMIT 2")

for row in cur.iterate():
    print(row)
# [ 1, 'some text', datetime.datetime(2014, 5, 18, 6, 47, 1, 928014) ]
# [ 2, 'something else', None ]

```
Streaming is recommended if you want to further process each row, save the results in a non-list/dict format (e.g. Pandas DataFrame), or save the results in a file.


**In-memory results as list**:

```python
cur = connection.cursor()
cur.execute("SELECT * FROM a_table LIMIT 2")
cur.fetchall()
# [ [1, 'something'], [2, 'something_else'] ]
```


**In-memory results as dictionary**:

```python
cur = connection.cursor('dict')
cur.execute("SELECT * FROM a_table LIMIT 2")
cur.fetchall()
# [ {'id': 1, 'value': 'something'}, {'id': 2, 'value': 'something_else'} ]
connection.close()
```


**Query using named parameters**:

```python
# Using named parameter bindings requires psycopg2>=2.5.1 which is not includes with the base vertica_python requirements.

cur = connection.cursor()
cur.execute("SELECT * FROM a_table WHERE a = :propA b = :propB", {'propA': 1, 'propB': 'stringValue'})

cur.fetchall()
# [ [1, 'something'], [2, 'something_else'] ]
```

**Insert and commits** :

```python
cur = connection.cursor()

# inline commit
cur.execute("INSERT INTO a_table (a, b) VALUES (1, 'aa'); commit;")

# commit in execution
cur.execute("INSERT INTO a_table (a, b) VALUES (1, 'aa')")
cur.execute("INSERT INTO a_table (a, b) VALUES (2, 'bb')")
cur.execute("commit;")

# connection.commit()
cur.execute("INSERT INTO a_table (a, b) VALUES (1, 'aa')")
connection.commit()
```


**Copy** :

```python
cur = connection.cursor()
cur.copy("COPY test_copy (id, name) from stdin DELIMITER ',' ",  csv)
```

Where `csv` is either a string or a file-like object (specifically, any object with a `read()` method). If using a file, the data is streamed.



## Rowcount oddities

vertica_python behaves a bit differently than dbapi when returning rowcounts.

After a select execution, the rowcount will be -1, indicating that the row count is unknown. The rowcount value will be updated as data is streamed.

```python
cur.execute('SELECT 10 things')

cur.rowcount == -1  # indicates unknown rowcount

cur.fetchone()
cur.rowcount == 1
cur.fetchone()
cur.rowcount == 2
cur.fetchall()
cur.rowcount == 10
```

After an insert/update/delete, the rowcount will be returned as a single element row:

```python
cur.execute("DELETE 3 things")

cur.rowcount == -1  # indicates unknown rowcount
cur.fetchone()[0] == 3
```

## Nextset

If you execute multiple statements in a single call to execute(), you can use cursor.nextset() to retrieve all of the data.

```python
cur.execute('SELECT 1; SELECT 2;')

cur.fetchone()
# [1]
cur.fetchone()
# None

cur.nextset()
# True

cur.fetchone()
# [2]
cur.fetchone()
# None

cur.nextset()
# None
```

## UTF-8 encoding issues

While Vertica expects varchars stored to be UTF-8 encoded, sometimes invalid strings get into the database. You can specify how to handle reading these characters using the unicode_error connection option. This uses the same values as the unicode type (https://docs.python.org/2/library/functions.html#unicode)

```python
cur = vertica_python.Connection({..., 'unicode_error': 'strict'}).cursor()
cur.execute(r"SELECT E'\xC2'")
cur.fetchone()
# caught 'utf8' codec can't decode byte 0xc2 in position 0: unexpected end of data

cur = vertica_python.Connection({..., 'unicode_error': 'replace'}).cursor()
cur.execute(r"SELECT E'\xC2'")
cur.fetchone()
# �

cur = vertica_python.Connection({..., 'unicode_error': 'ignore'}).cursor()
cur.execute(r"SELECT E'\xC2'")
cur.fetchone()
# 
```

## License

Apache 2.0 License, please see `LICENSE` for details.


## Acknowledgements

We would like to thank the contributors to the Ruby Vertica gem (https://github.com/sprsquish/vertica), as this project gave us inspiration and help in understanding Vertica's wire protocol. These contributors are:

 * [Matt Bauer](http://github.com/mattbauer)
 * [Jeff Smick](http://github.com/sprsquish)
 * [Willem van Bergen](http://github.com/wvanbergen)
 * [Camilo Lopez](http://github.com/camilo)
