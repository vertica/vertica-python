# Copyright (c) 2018-2023 Micro Focus or one of its affiliates.
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

from ...mixins.notice_response_attr import _NoticeResponseAttrMixin
from ..message import BackendMessage


class NoticeResponse(_NoticeResponseAttrMixin, BackendMessage):
    message_id = b'N'

    def __init__(self, data):
        BackendMessage.__init__(self)
        # `_notice_attrs` is required by _NoticeResponseAttrMixin and also used
        # by QueryError
        self._notice_attrs = NoticeResponse._unpack_data(data)

    def error_message(self):
        return ', '.join([
            "{0}: {1}".format(name, value)
            for (name, value) in self.values.items()
        ])

    def __str__(self):
        return "NoticeResponse: {}".format(self.error_message())

    @property
    def values(self):
        """
        A mapping of server-provided values describing this notice.

        The keys of this mapping are user-facing strings. The contents of any
        given NoticeResponse can vary based on the context or version of
        Vertica.

        For access to specific values, the appropriate property getter is
        recommended.

        Example return value:

        ```
            {
                'Severity': 'ERROR',
                'Message': 'Syntax error at or near "foobar"',
                'Sqlstate': '42601',
                'Position': '1',
                'Routine': 'base_yyerror',
                'File': '/data/.../vertica/Parser/scan.l',
                'Line': '1043',
                'Error Code': '4856'
            }
        ```
        """
        return self._get_labeled_values()

    @staticmethod
    def _unpack_data(data):
        data_mapping = {}

        pos = 0
        while pos < len(data) - 1:
            null_byte = data.find(b'\x00', pos)

            unpacked = unpack_from('c{0}sx'.format(null_byte - 1 - pos), data, pos)
            key = unpacked[0]
            value = unpacked[1]
            data_mapping[key] = value.decode('utf-8', 'replace')

            pos += (len(value) + 2)

        return data_mapping

BackendMessage.register(NoticeResponse)
