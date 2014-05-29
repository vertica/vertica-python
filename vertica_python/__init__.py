from __future__ import absolute_import

import re

from datetime import datetime, date, time
from decimal import Decimal

from vertica_python.vertica.connection import Connection

# dbApi types
from vertica_python.datatypes import Binary, STRING, BINARY, NUMBER, DATETIME, ROWID
from vertica_python.datatypes import Date, Time, Timestamp
from vertica_python.datatypes import DateFromTicks, TimeFromTicks, TimestampFromTicks

# dbApi errors
from vertica_python.errors import Error, Warning, DataError, DatabaseError, ProgrammingError
from vertica_python.errors import IntegrityError, InterfaceError, InternalError
from vertica_python.errors import NotSupportedError, OperationalError

# Custom errors
from vertica_python.errors import TimedOutError, ConnectionError, SSLNotSupported, MessageError, EmptyQueryError, QueryError


# Main module for this library.

# The version number of this library.
version_info = (0, 2, 2)
__version__ = '.'.join(map(str, version_info))

__author__ = 'Uber Technologies, Inc'
__copyright__ = 'Copyright 2013, Uber Technologies, Inc.'
__license__ = 'MIT'

# The protocol version (3.0.0) implemented in this library.
PROTOCOL_VERSION = 3 << 16


apilevel = 2.0

# Threads may share the module, but not connections!
threadsafety = 1
paramstyle = 'named' # WHERE name=:name



# Opens a new connection to a Vertica database.
def connect(options):
    return Connection(options)


# Properly quotes a value for safe usage in SQL queries.
#
# This method has quoting rules for common types. Any other object will be converted to
# a string using unicode() and then quoted as a string.
#
#def quote(value):
#    if value is None:
#        return 'NULL'
#    elif value is False:
#        return 'FALSE'
#    elif value is True:
#        return 'TRUE'
#    elif isinstance(value, datetime) or isinstance(value, time):
#        return value.strftime("'%Y-%m-%d %H:%M:%S'::timestamp")
#    elif isinstance(value, date):
#        return value.strftime("'%Y-%m-%d'::date")
#    elif isinstance(value, basestring) or isinstance(value, unicode):
#        return "'{0}'".format(re.sub(r"'", "''", value))
#    elif isinstance(value, Decimal) or isinstance(value, int) or isinstance(value, long) or isinstance(value, float):
#        return str(value)
#    elif isinstance(value, list):
#        return map(lambda x: quote(x), value)
#    else:
#        return quote(unicode(value))
#
#
## Quotes an identifier for safe use within SQL queries, using double quotes.
#def quote_identifier(identifier):
#    return "\"{0}\"".format(re.sub(r'\"', '""', unicode(identifier)))
#

