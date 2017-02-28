

from vertica_python.vertica.messages.message import BackendMessage


class EmptyQueryResponse(BackendMessage):
    def __init__(self, data=None):
        self.data = data


EmptyQueryResponse._message_id(b'I')
