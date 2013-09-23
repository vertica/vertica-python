from __future__ import absolute_import

from struct import pack

from vertica_python.vertica.messages.message import FrontendMessage

class Query(FrontendMessage):

    def __init__(self, query_string):
        self.query_string = query_string

    def to_bytes(self):
        return self.message_string(pack('{0}sx'.format(len(self.query_string)), self.query_string.encode('utf-8')))


Query._message_id('Q')
