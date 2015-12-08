

from vertica_python.vertica.messages.message import FrontendMessage


class CopyDone(FrontendMessage):
    pass


CopyDone._message_id(b'c')
