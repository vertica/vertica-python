from __future__ import print_function, division, absolute_import

from struct import pack

from ..message import BulkFrontendMessage


class SslRequest(BulkFrontendMessage):
    message_id = None
    SSL_REQUEST = 80877103

    def read_bytes(self):
        bytes_ = pack('!I', self.SSL_REQUEST)
        return bytes_
