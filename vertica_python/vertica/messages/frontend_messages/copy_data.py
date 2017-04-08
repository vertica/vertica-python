from __future__ import print_function, division, absolute_import

from struct import pack

from ..message import BulkFrontendMessage

UTF_8 = 'utf-8'


class CopyData(BulkFrontendMessage):
    message_id = b'd'

    def __init__(self, data):
        BulkFrontendMessage.__init__(self)
        self._data = data

    def read_bytes(self):
        # to deal with unicode strings
        encoded = self._data.encode(UTF_8)
        bytes_ = pack('{0}s'.format(len(encoded)), encoded)
        return bytes_
