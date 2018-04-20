# Copyright (c) 2013-2017 Uber Technologies, Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function, division, absolute_import

from .vertica.connection import Connection, connect

# Importing exceptions for compatibility with dbapi 2.0.
# See: PEP 249 - Python Database API 2.0
#      https://www.python.org/dev/peps/pep-0249/#exceptions
from . import errors
from .errors import (
    Error, Warning, DataError, DatabaseError, IntegrityError, InterfaceError,
    InternalError, NotSupportedError, OperationalError, ProgrammingError)

# Main module for this library.
__author__ = 'Uber Technologies, Inc'
__copyright__ = 'Copyright 2013, Uber Technologies, Inc.'
__license__ = 'MIT'

__all__ = ['Connection', 'PROTOCOL_VERSION', 'version_info', 'apilevel', 'threadsafety',
           'paramstyle', 'connect', 'Error', 'Warning', 'DataError', 'DatabaseError',
           'IntegrityError', 'InterfaceError', 'InternalError', 'NotSupportedError',
           'OperationalError', 'ProgrammingError']

# The version number of this library.
version_info = (0, 7, 3)
__version__ = '.'.join(map(str, version_info))

# The protocol version (3.0.0) implemented in this library.
PROTOCOL_VERSION = 3 << 16

apilevel = 2.0
threadsafety = 1  # Threads may share the module, but not connections!
paramstyle = 'named'  # WHERE name=:name
