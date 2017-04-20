from __future__ import print_function, division, absolute_import

from struct import unpack

from ..message import BackendMessage


class ReadyForQuery(BackendMessage):
    message_id = b'Z'

    STATUSES = {
        b'I': 'no_transaction',
        b'T': 'in_transaction',
        b'E': 'failed_transaction'
    }

    def __init__(self, data):
        BackendMessage.__init__(self)
        self.transaction_status = self.STATUSES[unpack('c', data)[0]]


BackendMessage.register(ReadyForQuery)
