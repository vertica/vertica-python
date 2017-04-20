from __future__ import print_function, division, absolute_import

from vertica_python.vertica.connection import Connection

# Importing exceptions for compatibility with dbapi 2.0.
# See: PEP 249 - Python Database API 2.0
#      https://www.python.org/dev/peps/pep-0249/#exceptions
from vertica_python import errors
from vertica_python.errors import (
    Error, Warning, DataError, DatabaseError, IntegrityError, InterfaceError,
    InternalError, NotSupportedError, OperationalError, ProgrammingError)

# Main module for this library.
__author__ = 'Uber Technologies, Inc'
__copyright__ = 'Copyright 2013, Uber Technologies, Inc.'
__license__ = 'MIT'

__all__ = ['Connection', 'PROTOCOL_VERSION', 'version_info', 'apilevel', 'threadsafety',
           'paramstyle', 'connect', 'Error', 'Warning', 'DataError', 'DatabaseError',
           'IntegrityError', 'InterfaceError, InternalError', 'NotSupportedError',
           'OperationalError', 'ProgrammingError']

# The version number of this library.
version_info = (0, 6, 14)
__version__ = '.'.join(map(str, version_info))

# The protocol version (3.0.0) implemented in this library.
PROTOCOL_VERSION = 3 << 16

apilevel = 2.0
threadsafety = 1  # Threads may share the module, but not connections!
paramstyle = 'named'  # WHERE name=:name


def connect(**kwargs):
    """Opens a new connection to a Vertica database."""
    return Connection(kwargs)
