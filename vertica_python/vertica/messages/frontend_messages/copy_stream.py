# Copyright (c) 2013-2017 Uber Technologies, Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function, division, absolute_import

from six import text_type, binary_type

from ..message import StreamFrontendMessage

DEFAULT_BUFFER_SIZE = 131072

UTF_8 = 'utf-8'


class CopyStream(StreamFrontendMessage):
    message_id = b'd'

    def __init__(self, stream, buffer_size=DEFAULT_BUFFER_SIZE, unicode_error='strict'):
        StreamFrontendMessage.__init__(self)
        self._stream = stream
        self._unicode_error = unicode_error
        self._buffer_size = buffer_size

    def stream_bytes(self):
        while True:
            chunk = self._stream.read(self._buffer_size)
            if isinstance(chunk, text_type):
                bytes_ = chunk.encode(encoding=UTF_8, errors=self._unicode_error)
            elif isinstance(chunk, binary_type):
                bytes_ = chunk
            else:
                raise TypeError("should be string or bytes")

            if not chunk:
                break

            yield bytes_
