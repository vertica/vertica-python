# Copyright (c) 2018-2020 Micro Focus or one of its affiliates.
# Copyright (c) 2018 Uber Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Copyright (c) 2013-2017 Uber Technologies, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

from __future__ import print_function, division, absolute_import

from six import text_type, binary_type

from ..message import StreamFrontendMessage

DEFAULT_BUFFER_SIZE = 131072


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
                bytes_ = chunk.encode(encoding='utf-8', errors=self._unicode_error)
            elif isinstance(chunk, binary_type):
                bytes_ = chunk
            else:
                raise TypeError("should be string or bytes")

            if not chunk:
                break

            yield bytes_
