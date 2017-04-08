from __future__ import print_function, division, absolute_import

from ..message import StreamFrontendMessage

DEFAULT_BUFFER_SIZE = 131072


class CopyStream(StreamFrontendMessage):
    message_id = b'd'

    def __init__(self, stream, buffer_size=DEFAULT_BUFFER_SIZE):
        StreamFrontendMessage.__init__(self)
        self._stream = stream
        self._buffer_size = buffer_size

    def stream_bytes(self):
        while True:
            bytes_ = self._stream.read(self._buffer_size)
            if not bytes_:
                break
            yield bytes_
