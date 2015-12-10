

from struct import pack

from vertica_python.vertica.messages.message import FrontendMessage


class CopyData(FrontendMessage):

    def __init__(self, data):
        self.data = data

    def to_bytes(self):
        # to deal with unicode strings
        encoded = self.data.encode('utf-8')
        return self.message_string(pack('{0}s'.format(len(encoded)), encoded))


CopyData._message_id(b'd')
