from __future__ import absolute_import

from struct import pack

from vertica_python.vertica.messages.message import FrontendMessage


class Query(FrontendMessage):

    def __init__(self, query_string):
        self.query_string = query_string

    def to_bytes(self):
        s = self.query_string
        if isinstance(s, str):
                s = unicode(s, 'utf-8')
        encoded = s.encode('utf-8')
        return self.message_string(pack('{0}sx'.format(len(encoded)), encoded))


Query._message_id('Q')
