

from struct import pack

from vertica_python.vertica.messages.message import FrontendMessage

class CopyStream(FrontendMessage):

    def __init__(self, stream, buffer_size=131072):
        self.stream = stream
        self.bufsize = buffer_size

    def read_bytes(self):

        data = self.stream.read(self.bufsize)

        if len(data) == 0:
            return data

        return self.message_string(data)

CopyStream._message_id(b'd')
