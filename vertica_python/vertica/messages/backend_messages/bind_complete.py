

from vertica_python.vertica.messages.message import BackendMessage


class BindComplete(BackendMessage):
    pass


BindComplete._message_id(b'2')
