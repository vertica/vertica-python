

from struct import pack

from vertica_python.vertica.messages.message import FrontendMessage


class SslRequest(FrontendMessage):

    def to_bytes(self):
        return self.message_string(pack('!I', 80877103))


SslRequest._message_id(None)
