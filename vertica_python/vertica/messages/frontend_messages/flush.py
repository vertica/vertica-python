

from vertica_python.vertica.messages.message import FrontendMessage


class Flush(FrontendMessage):
    pass


Flush._message_id(b'H')
