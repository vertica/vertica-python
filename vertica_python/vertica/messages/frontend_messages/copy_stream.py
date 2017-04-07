from __future__ import print_function, division, absolute_import

from ..message import FrontendMessage


class CopyStream(FrontendMessage):
    message_id = b'd'

    def __init__(self, stream, buffer_size=131072):
        FrontendMessage.__init__(self)
        self.stream = stream
        self.bufsize = buffer_size

    def read_bytes(self):
        data = self.stream.read(self.bufsize)

        if len(data) == 0:
            return data

        return self.message_string(data)
