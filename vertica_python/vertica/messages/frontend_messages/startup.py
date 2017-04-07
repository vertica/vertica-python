from __future__ import print_function, division, absolute_import

import platform
import os
import uuid
from struct import pack

# noinspection PyUnresolvedReferences
from six.moves.builtins import str

import vertica_python
from ..message import FrontendMessage

ASCII = 'ascii'


class Startup(FrontendMessage):
    message_id = None

    def __init__(self, user, database, options=None):
        FrontendMessage.__init__(self)

        self.user = user
        self.database = database
        self.options = options
        self.type = b'vertica-python'
        self.version = vertica_python.__version__.encode(ASCII)
        self.platform = platform.platform().encode(ASCII)
        self.pid = '{0}'.format(os.getpid()).encode(ASCII)
        self.label = self.type + b'-' + self.version + b'-' + str(uuid.uuid1()).encode(ASCII)

    def to_bytes(self):
        startstr = pack('!I', vertica_python.PROTOCOL_VERSION)
        if self.user is not None:
            startstr += pack('4sx{0}sx'.format(len(self.user)), b'user', self.user)
        if self.database is not None:
            startstr += pack('8sx{0}sx'.format(len(self.database)), b'database', self.database)
        if self.options is not None:
            startstr += pack('7sx{0}sx'.format(len(self.options)), b'options', self.options)
        startstr += pack('12sx{0}sx'.format(len(self.label)), b'client_label', self.label)
        startstr += pack('11sx{0}sx'.format(len(self.type)), b'client_type', self.type)
        startstr += pack('14sx{0}sx'.format(len(self.version)), b'client_version', self.version)
        startstr += pack('9sx{0}sx'.format(len(self.platform)), b'client_os', self.platform)
        startstr += pack('10sx{0}sx'.format(len(self.pid)), b'client_pid', self.pid)
        startstr += pack('x')
        return self.message_string(startstr)
