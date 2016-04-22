

from vertica_python.vertica.messages.message import BackendMessage


class NoData(BackendMessage):
    pass


NoData._message_id(b'n')
