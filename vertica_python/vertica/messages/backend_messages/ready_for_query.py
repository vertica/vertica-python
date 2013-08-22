from __future__ import absolute_import

from struct import unpack

from vertica_python.vertica.messages.message import BackendMessage

class ReadyForQuery(BackendMessage):

    STATUSES = {
        'I': 'no_transaction',
        'T': 'in_transaction',
        'E': 'failed_transaction'
    }

    def __init__(self, data):
        self.transaction_status = self.STATUSES[unpack('c', data)[0]]


ReadyForQuery._message_id('Z')
