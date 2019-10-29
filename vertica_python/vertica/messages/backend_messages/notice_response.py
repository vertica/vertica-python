# Copyright (c) 2018-2019 Micro Focus or one of its affiliates.
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

import collections
import six
from struct import unpack_from

from ..message import BackendMessage

FIELD_DEFINITIONS = [
    {'type': b'q', 'name': "Internal Query", 'attribute': 'internal_query'},
    {'type': b'S', 'name': "Severity", 'attribute': 'severity'},
    {'type': b'M', 'name': "Message", 'attribute': 'message'},
    {'type': b'C', 'name': "Sqlstate", 'attribute': 'sqlstate'},
    {'type': b'D', 'name': "Detail", 'attribute': 'detail'},
    {'type': b'H', 'name': "Hint", 'attribute': 'hint'},
    {'type': b'P', 'name': "Position", 'attribute': 'position'},
    {'type': b'W', 'name': "Where", 'attribute': 'where'},
    {'type': b'p', 'name': "Internal Position", 'attribute': 'internal_position'},
    {'type': b'R', 'name': "Routine", 'attribute': 'routine'},
    {'type': b'F', 'name': "File", 'attribute': 'file'},
    {'type': b'L', 'name': "Line", 'attribute': 'line'},
    {'type': b'V', 'name': "Error Code", 'attribute': 'error_code'}
]
FIELD_TYPE_TO_NAME = {field['type']: field['name'] for field in FIELD_DEFINITIONS}
FIELD_ATTR_TO_TYPE = {field['attribute']: field['type'] for field in FIELD_DEFINITIONS}


class _NoticeResponseAttrMixin:
    # class must have `self._notice_attrs` property that provides a mapping from
    # the type indicator (see `FIELD_DEFINITIONS`) to value.

    @property
    def internal_query(self):
        return self._notice_attrs.get(FIELD_ATTR_TO_TYPE['internal_query'])

    @property
    def severity(self):
        return self._notice_attrs.get(FIELD_ATTR_TO_TYPE['severity'])

    @property
    def message(self):
        return self._notice_attrs.get(FIELD_ATTR_TO_TYPE['message'])

    @property
    def sqlstate(self):
        return self._notice_attrs.get(FIELD_ATTR_TO_TYPE['sqlstate'])

    @property
    def detail(self):
        return self._notice_attrs.get(FIELD_ATTR_TO_TYPE['detail'])

    @property
    def hint(self):
        return self._notice_attrs.get(FIELD_ATTR_TO_TYPE['hint'])

    @property
    def position(self):
        return self._notice_attrs.get(FIELD_ATTR_TO_TYPE['position'])

    @property
    def where(self):
        return self._notice_attrs.get(FIELD_ATTR_TO_TYPE['where'])

    @property
    def internal_position(self):
        return self._notice_attrs.get(FIELD_ATTR_TO_TYPE['internal_position'])

    @property
    def routine(self):
        return self._notice_attrs.get(FIELD_ATTR_TO_TYPE['routine'])

    @property
    def file(self):
        return self._notice_attrs.get(FIELD_ATTR_TO_TYPE['file'])

    @property
    def line(self):
        return self._notice_attrs.get(FIELD_ATTR_TO_TYPE['line'])

    @property
    def error_code(self):
        return self._notice_attrs.get(FIELD_ATTR_TO_TYPE['error_code'])

    def _get_labeled_values(self):
        values_mapping = collections.OrderedDict()

        for (typ, name) in six.iteritems(FIELD_TYPE_TO_NAME):
            if typ in self._notice_attrs:
                values_mapping[name] = self._notice_attrs[typ]

        return values_mapping


class NoticeResponse(_NoticeResponseAttrMixin, BackendMessage):
    message_id = b'N'

    def __init__(self, data):
        BackendMessage.__init__(self)
        # `_notice_attrs` is required by _NoticeResponseAttrMixin and also used
        # by QueryError 
        # Caution: this private attr is used internally by `QueryError`
        self._notice_attrs = NoticeResponse._unpack_data(data)

    def error_message(self):
        return ', '.join([
            "{0}: {1}".format(name, value)
            for (name, value) in six.iteritems(self.values)
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
            data_mapping[key] = value.decode('utf-8')

            pos += (len(value) + 2)

        return data_mapping

BackendMessage.register(NoticeResponse)
