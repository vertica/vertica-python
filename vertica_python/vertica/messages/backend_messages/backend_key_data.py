

from struct import unpack

from vertica_python.vertica.messages.message import BackendMessage


class BackendKeyData(BackendMessage):

    def __init__(self, data):
        unpacked = unpack('!2I', data)
        self.pid = unpacked[0]
        self.key = unpacked[1]


BackendKeyData._message_id(b'K')
