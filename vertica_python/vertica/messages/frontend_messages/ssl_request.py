from __future__ import print_function, division, absolute_import

from struct import pack

from ..message import FrontendMessage


class SslRequest(FrontendMessage):
    message_id = None

    def to_bytes(self):
        return self.message_string(pack('!I', 80877103))
