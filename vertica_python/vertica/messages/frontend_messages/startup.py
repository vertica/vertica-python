

from struct import pack

import vertica_python
import platform
import os
import uuid
from vertica_python.vertica.messages.message import FrontendMessage


class Startup(FrontendMessage):

    def __init__(self, user, database, options=None):
        self.user = user
        self.database = database
        self.options = options
        self.type = b'vertica-python'
        self.version = vertica_python.__version__.encode('ascii')
        self.platform = platform.platform().encode('ascii')
        self.pid = '{0}'.format(os.getpid()).encode('ascii')
        self.label = self.type+b'-'+self.version+b'-'+str(uuid.uuid1()).encode('ascii')

    def to_bytes(self):
        startstr = pack('!I', vertica_python.PROTOCOL_VERSION)
        if self.user is not None:
            startstr = startstr + pack('4sx{0}sx'.format(len(self.user)), b'user', self.user)
        if self.database is not None:
            startstr = startstr + pack('8sx{0}sx'.format(len(self.database)), b'database', self.database)
        if self.options is not None:
            startstr = startstr + pack('7sx{0}sx'.format(len(self.options)), b'options', self.options)
        startstr = startstr + pack('12sx{0}sx'.format(len(self.label)), b'client_label', self.label)
        startstr = startstr + pack('11sx{0}sx'.format(len(self.type)), b'client_type', self.type)
        startstr = startstr + pack('14sx{0}sx'.format(len(self.version)), b'client_version', self.version)
        startstr = startstr + pack('9sx{0}sx'.format(len(self.platform)), b'client_os', self.platform)
        startstr = startstr + pack('10sx{0}sx'.format(len(self.pid)), b'client_pid', self.pid)
        startstr = startstr + pack('x')
        return self.message_string(startstr)


Startup._message_id(None)
