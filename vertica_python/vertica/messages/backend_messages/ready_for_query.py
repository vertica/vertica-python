

from struct import unpack

from vertica_python.vertica.messages.message import BackendMessage


class ReadyForQuery(BackendMessage):

    STATUSES = {
        b'I': 'no_transaction',
        b'T': 'in_transaction',
        b'E': 'failed_transaction'
    }

    def __init__(self, data):
        self.transaction_status = self.STATUSES[unpack('c', data)[0]]


ReadyForQuery._message_id(b'Z')
