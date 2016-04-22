

from struct import pack

import vertica_python
from vertica_python.vertica.messages.message import FrontendMessage


class Startup(FrontendMessage):

    def __init__(self, user, database, options=None):
        self.user = user
        self.database = database
        self.options = options

    def to_bytes(self):
        startstr = pack('!I', vertica_python.PROTOCOL_VERSION)
        if self.user is not None:
            startstr = startstr + pack('4sx{0}sx'.format(len(self.user)), b'user', self.user)
        if self.database is not None:
            startstr = startstr + pack('8sx{0}sx'.format(len(self.database)), b'database', self.database)
        if self.options is not None:
            startstr = startstr + pack('7sx{0}sx'.format(len(self.options)), b'options', self.options)
        startstr = startstr + pack('x')
        return self.message_string(startstr)


Startup._message_id(None)
