from __future__ import print_function, division, absolute_import

import platform
import os
import uuid
from struct import pack

# noinspection PyUnresolvedReferences,PyCompatibility
from builtins import str

import vertica_python
from ..message import BulkFrontendMessage

ASCII = 'ascii'


class Startup(BulkFrontendMessage):
    message_id = None

    def __init__(self, user, database, options=None):
        BulkFrontendMessage.__init__(self)

        self._user = user
        self._database = database
        self._options = options
        self._type = b'vertica-python'
        self._version = vertica_python.__version__.encode(ASCII)
        self._platform = platform.platform().encode(ASCII)
        self._pid = '{0}'.format(os.getpid()).encode(ASCII)
        self._label = self._type + b'-' + self._version + b'-' + str(uuid.uuid1()).encode(ASCII)

    def read_bytes(self):
        bytes_ = pack('!I', vertica_python.PROTOCOL_VERSION)
        if self._user is not None:
            bytes_ += pack('4sx{0}sx'.format(len(self._user)), b'user', self._user)
        if self._database is not None:
            bytes_ += pack('8sx{0}sx'.format(len(self._database)), b'database', self._database)
        if self._options is not None:
            bytes_ += pack('7sx{0}sx'.format(len(self._options)), b'options', self._options)
        bytes_ += pack('12sx{0}sx'.format(len(self._label)), b'client_label', self._label)
        bytes_ += pack('11sx{0}sx'.format(len(self._type)), b'client_type', self._type)
        bytes_ += pack('14sx{0}sx'.format(len(self._version)), b'client_version', self._version)
        bytes_ += pack('9sx{0}sx'.format(len(self._platform)), b'client_os', self._platform)
        bytes_ += pack('10sx{0}sx'.format(len(self._pid)), b'client_pid', self._pid)
        bytes_ += pack('x')

        return bytes_
