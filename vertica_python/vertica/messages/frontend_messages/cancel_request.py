from __future__ import print_function, division, absolute_import

from struct import pack

from ..message import FrontendMessage


class CancelRequest(FrontendMessage):
    message_id = None

    def __init__(self, backend_pid, backend_key):
        FrontendMessage.__init__(self)
        self.backend_pid = backend_pid
        self.backend_key = backend_key

    def to_bytes(self):
        return self.message_string(pack('!3I', 80877102, self.backend_pid, self.backend_key))
