# vertica-python

vertica-python is a native Python adapter for the Vertica (http://www.vertica.com) database.

This package is a Python port of the excellent Vertica Ruby gem (https://github.com/sprsquish/vertica).

vertica-python is currently in a alpha stage; it has been tested for functionality, but does not have a test suite. Please use with caution, and feel free to submit issues and/or pull requests.

vertica-python has been tested with Vertica 6.1.2 and Python 2.7.5. Please let me know if it's working on other versions.


## Installation

    pip install vertica-python

Source code for vertica-python can be found at:

    http://github.com/uber/vertica-python

## Usage


**Buffered** (in-memory) results:

```
from vertica_python import connect

connection = connect({
    'host': '127.0.0.1',
    'port': 5433,
    'user': 'some_user',
    'password': 'some_password',
    'database': 'a_database'

    })

result = connection.query("SELECT * FROM a_table LIMIT 2")
connection.close()

print result.rows() 
# [{'id': 1, 'value': 'something'}, {'id': 2, 'value': 'something_else'}]

```

**Unbuffered** (streaming) results:

```
from vertica_python import connect

connection = connect({
    'host': '127.0.0.1',
    'port': 5433,
    'user': 'some_user',
    'password': 'some_password',
    'database': 'a_database'

    })

def magical_row_handler(row):
    print row

result = connection.query("SELECT * FROM a_table LIMIT 2", options={}, handler=magical_row_jhandler)
# {'id': 1, 'value': 'something'}
# {'id': 2, 'value': 'something_else'}

connection.close()

```

## License

MIT License, please see `LICENSE` for details.


## Acknowledgements

Many thanks go to the contributors to the Ruby Vertica gem, since they did all of the wrestling with Vertica's protocol and have kept the gem updated. They are:

 * [Matt Bauer](http://github.com/mattbauer)
 * [Jeff Smick](http://github.com/sprsquish)
 * [Willem van Bergen](http://github.com/wvanbergen)
 * [Camilo Lopez](http://github.com/camilo)
