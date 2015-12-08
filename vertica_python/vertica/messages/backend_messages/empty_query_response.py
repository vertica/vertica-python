

from vertica_python.vertica.messages.message import BackendMessage


class EmptyQueryResponse(BackendMessage):
    pass


EmptyQueryResponse._message_id(b'I')
