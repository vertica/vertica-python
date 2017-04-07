from __future__ import print_function, division, absolute_import

from struct import pack

from ..message import FrontendMessage


class Execute(FrontendMessage):
    message_id = b'E'

    def __init__(self, portal_name, max_rows):
        FrontendMessage.__init__(self)
        self.portal_name = portal_name
        self.max_rows = max_rows

    def to_bytes(self):
        return self.message_string(
            pack('!{0}sxI'.format(len(self.portal_name)), self.portal_name, self.max_rows))
