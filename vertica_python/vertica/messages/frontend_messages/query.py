from __future__ import print_function, division, absolute_import

from struct import pack

from ..message import BulkFrontendMessage

UTF_8 = 'utf-8'


class Query(BulkFrontendMessage):
    message_id = b'Q'

    def __init__(self, query_string):
        BulkFrontendMessage.__init__(self)
        self._query_string = query_string

    def read_bytes(self):
        encoded = self._query_string.encode(UTF_8)
        bytes_ = pack('{0}sx'.format(len(encoded)), encoded)
        return bytes_
