from __future__ import print_function, division, absolute_import

from struct import pack

from ..message import FrontendMessage


class CopyData(FrontendMessage):
    message_id = b'd'

    def __init__(self, data):
        FrontendMessage.__init__(self)
        self.data = data

    def to_bytes(self):
        # to deal with unicode strings
        encoded = self.data.encode('utf-8')
        return self.message_string(pack('{0}s'.format(len(encoded)), encoded))
