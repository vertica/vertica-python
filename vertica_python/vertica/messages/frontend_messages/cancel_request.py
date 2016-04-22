

from struct import pack

from vertica_python.vertica.messages.message import FrontendMessage


class CancelRequest(FrontendMessage):

    def __init__(self, backend_pid, backend_key):
        self.backend_pid = backend_pid
        self.backend_key = backend_key

    def to_bytes(self):
        return self.message_string(pack('!3I', 80877102, self.backend_pid, self.backend_key))


CancelRequest._message_id(None)
