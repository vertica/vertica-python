from __future__ import print_function, division, absolute_import

from struct import pack

from ..message import BulkFrontendMessage


class Close(BulkFrontendMessage):
    message_id = b'C'

    def __init__(self, close_type, close_name):
        BulkFrontendMessage.__init__(self)

        self._close_name = close_name

        if close_type == 'portal':
            self._close_type = 'P'
        elif close_type == 'prepared_statement':
            self._close_type = 'S'
        else:
            raise ValueError("{0} is not a valid close_type. "
                             "Must be either portal or prepared_statement".format(close_type))

    def read_bytes(self):
        bytes_ = pack('c{0}sx'.format(len(self._close_name)), self._close_type, self._close_name)
        return bytes_
