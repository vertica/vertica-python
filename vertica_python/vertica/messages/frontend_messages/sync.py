

from vertica_python.vertica.messages.message import FrontendMessage


class Sync(FrontendMessage):
    pass


Sync._message_id(b'S')
