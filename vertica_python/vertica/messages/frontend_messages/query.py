from __future__ import print_function, division, absolute_import

from struct import pack

from ..message import FrontendMessage


class Query(FrontendMessage):
    message_id = b'Q'

    def __init__(self, query_string):
        FrontendMessage.__init__(self)
        self.query_string = query_string

    def to_bytes(self):
        encoded = self.query_string.encode('utf-8')
        return self.message_string(pack('{0}sx'.format(len(encoded)), encoded))
