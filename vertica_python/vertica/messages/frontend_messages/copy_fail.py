from __future__ import print_function, division, absolute_import

from struct import pack

from ..message import BulkFrontendMessage


class CopyFail(BulkFrontendMessage):
    message_id = b'f'

    def __init__(self, error_message):
        BulkFrontendMessage.__init__(self)
        self._error_message = error_message

    def read_bytes(self):
        bytes_ = pack('{0}sx'.format(len(self._error_message)), self._error_message)
        return bytes_
