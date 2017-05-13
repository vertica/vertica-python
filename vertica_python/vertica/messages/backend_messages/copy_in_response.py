from __future__ import print_function, division, absolute_import

from struct import unpack

from ..message import BackendMessage


class CopyInResponse(BackendMessage):
    message_id = b'G'

    def __init__(self, data):
        BackendMessage.__init__(self)
        values = unpack('!B{0}H'.format((len(data) - 1) // 2), data)
        self.format = values[0]
        self.column_formats = values[2::]


BackendMessage.register(CopyInResponse)
