# Copyright (c) 2018 Micro Focus or one of its affiliates.
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

from struct import unpack_from

from ..message import BackendMessage


class NoticeResponse(BackendMessage):
    message_id = b'N'

    FIELDS_DEFINITIONS = [
        {'type': b'q', 'name': "Internal Query", 'method': 'internal_query'},
        {'type': b'S', 'name': "Severity", 'method': 'severity'},
        {'type': b'M', 'name': "Message", 'method': 'message'},
        {'type': b'C', 'name': "Sqlstate", 'method': 'sqlstate'},
        {'type': b'D', 'name': "Detail", 'method': 'detail'},
        {'type': b'H', 'name': "Hint", 'method': 'hint'},
        {'type': b'P', 'name': "Position", 'method': 'position'},
        {'type': b'W', 'name': "Where", 'method': 'where'},
        {'type': b'p', 'name': "Internal Position", 'method': 'internal_position'},
        {'type': b'R', 'name': "Routine", 'method': 'routine'},
        {'type': b'F', 'name': "File", 'method': 'file'},
        {'type': b'L', 'name': "Line", 'method': 'line'},
        {'type': b'V', 'name': "Error Code", 'method': 'error_code'}
    ]

    def __init__(self, data):
        BackendMessage.__init__(self)
        self.values = {}

        pos = 0
        while pos < len(data) - 1:
            null_byte = data.find(b'\x00', pos)

            # This will probably work
            unpacked = unpack_from('c{0}sx'.format(null_byte - 1 - pos), data, pos)
            key = unpacked[0]
            value = unpacked[1]

            self.values[self.fields()[key]] = value
            pos += (len(value) + 2)

        # May want to break out into a function at some point
        for field_def in self.FIELDS_DEFINITIONS:
            if self.values.get(field_def['name'], None) is not None:
                setattr(self, field_def['method'], self.values[field_def['name']])

    def fields(self):
        # was FIELDS before
        # TODO verify that it doesn't break anything
        pairs = []
        for field in self.FIELDS_DEFINITIONS:
            pairs.append((field['type'], field['name']))
        return dict(pairs)

    def error_message(self):
        ordered = []
        for field in self.FIELDS_DEFINITIONS:
            if self.values.get(field['name']) is not None:
                ordered.append("{0}: {1}".format(field['name'], self.values[field['name']]))
        return ', '.join(ordered)

    def __str__(self):
        return "NoticeResponse: {}".format(self.error_message())


BackendMessage.register(NoticeResponse)
