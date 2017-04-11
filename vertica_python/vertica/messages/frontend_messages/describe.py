from __future__ import print_function, division, absolute_import

from struct import pack

from ..message import BulkFrontendMessage


class Describe(BulkFrontendMessage):
    message_id = b'D'

    def __init__(self, describe_type, describe_name):
        BulkFrontendMessage.__init__(self)

        self._describe_name = describe_name

        if describe_type == 'portal':
            self._describe_type = 'P'
        elif describe_type == 'prepared_statement':
            self._describe_type = 'S'
        else:
            raise ValueError("{0} is not a valid describe_type. "
                             "Must be either portal or prepared_statement".format(describe_type))

    def read_bytes(self):
        bytes_ = pack('c{0}sx'.format(len(self._describe_name)), self._describe_type,
                      self._describe_name)
        return bytes_
