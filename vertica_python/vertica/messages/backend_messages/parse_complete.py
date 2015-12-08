

from vertica_python.vertica.messages.message import BackendMessage


class ParseComplete(BackendMessage):
    pass


ParseComplete._message_id(b'1')
