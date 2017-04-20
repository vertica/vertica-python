from __future__ import print_function, division, absolute_import

from struct import pack

from ..message import BulkFrontendMessage


class CancelRequest(BulkFrontendMessage):
    message_id = None

    def __init__(self, backend_pid, backend_key):
        BulkFrontendMessage.__init__(self)
        self._backend_pid = backend_pid
        self._backend_key = backend_key

    def read_bytes(self):
        bytes_ = pack('!3I', 80877102, self._backend_pid, self._backend_key)
        return bytes_
