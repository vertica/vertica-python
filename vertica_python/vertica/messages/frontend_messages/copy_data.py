from __future__ import absolute_import

from struct import pack

from vertica_python.vertica.messages.message import FrontendMessage

class CopyData(FrontendMessage):

    def __init__(self, data):
        self.data = data

    def to_bytes(self):
        return self.message_string(pack('{0}s'.format(len(self.data)), self.data))


CopyData._message_id('d')
