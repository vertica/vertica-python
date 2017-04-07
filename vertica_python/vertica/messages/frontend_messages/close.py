from __future__ import print_function, division, absolute_import

from struct import pack

from ..message import FrontendMessage


class Close(FrontendMessage):
    message_id = b'C'

    def __init__(self, close_type, close_name):
        FrontendMessage.__init__(self)

        self.close_name = close_name

        if close_type == 'portal':
            self.close_type = 'P'
        elif close_type == 'prepared_statement':
            self.close_type = 'S'
        else:
            raise ValueError(
                "%s is not a valid close_type.  Must be either portal or prepared_statement",
                close_type)

    def to_bytes(self):
        return self.message_string(
            pack('c{0}sx'.format(len(self.close_name)), self.close_type, self.close_name))
