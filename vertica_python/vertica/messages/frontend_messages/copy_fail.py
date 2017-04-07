from __future__ import print_function, division, absolute_import

from struct import pack

from ..message import FrontendMessage


class CopyFail(FrontendMessage):
    message_id = b'f'

    def __init__(self, error_message):
        FrontendMessage.__init__(self)
        self.error_message = error_message

    def to_bytes(self):
        return self.message_string(
            pack('{0}sx'.format(len(self.error_message)), self.error_message))
