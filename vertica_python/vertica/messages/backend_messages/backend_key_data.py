from __future__ import print_function, division, absolute_import

from struct import unpack

from ..message import BackendMessage


class BackendKeyData(BackendMessage):
    message_id = b'K'

    def __init__(self, data):
        BackendMessage.__init__(self)
        unpacked = unpack('!2I', data)
        self.pid = unpacked[0]
        self.key = unpacked[1]


BackendMessage.register(BackendKeyData)
