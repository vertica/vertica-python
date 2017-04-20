from __future__ import print_function, division, absolute_import

from struct import unpack

from ..message import BackendMessage


class ParameterStatus(BackendMessage):
    message_id = b'S'

    def __init__(self, data):
        BackendMessage.__init__(self)
        null_byte = data.find(b'\x00')
        unpacked = unpack('{0}sx{1}sx'.format(null_byte - 1, len(data) - null_byte - 1), data)
        self.name = unpacked[0]
        self.value = unpacked[1]


BackendMessage.register(ParameterStatus)
