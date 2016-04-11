

from struct import unpack

from vertica_python.vertica.messages.message import BackendMessage


class CopyInResponse(BackendMessage):

    def __init__(self, data):
        values = unpack('!B{0}H'.format((len(data) - 1)//2), data)
        self.format = values[0]
        self.column_formats = values[2::]


CopyInResponse._message_id(b'G')
