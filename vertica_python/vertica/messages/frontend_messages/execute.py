from __future__ import print_function, division, absolute_import

from struct import pack

from ..message import BulkFrontendMessage


class Execute(BulkFrontendMessage):
    message_id = b'E'

    def __init__(self, portal_name, max_rows):
        BulkFrontendMessage.__init__(self)
        self._portal_name = portal_name
        self._max_rows = max_rows

    def read_bytes(self):
        bytes_ = pack('!{0}sxI'.format(len(self._portal_name)), self._portal_name, self._max_rows)
        return bytes_
