

from vertica_python.vertica.connection import Connection


# Main module for this library.

# The version number of this library.
version_info = (0, 6, 3)

__version__ = '.'.join(map(str, version_info))

__author__ = 'Uber Technologies, Inc'
__copyright__ = 'Copyright 2013, Uber Technologies, Inc.'
__license__ = 'MIT'

# The protocol version (3.0.0) implemented in this library.
PROTOCOL_VERSION = 3 << 16


apilevel = 2.0

# Threads may share the module, but not connections!
threadsafety = 1
paramstyle = 'named'  # WHERE name=:name


def connect(**kwargs):
    """Opens a new connection to a Vertica database."""
    return Connection(kwargs)
