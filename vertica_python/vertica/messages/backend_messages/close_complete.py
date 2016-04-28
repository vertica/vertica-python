

from vertica_python.vertica.messages.message import BackendMessage


class CloseComplete(BackendMessage):
    pass


CloseComplete._message_id(b'3')
