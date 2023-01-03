# Copyright (c) 2019-2023 Micro Focus or one of its affiliates.
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

from __future__ import print_function, division, absolute_import

from collections import OrderedDict

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
        values_mapping = OrderedDict()

        for field_def in FIELD_DEFINITIONS:
            if field_def['type'] in self._notice_attrs:
                values_mapping[field_def['name']] = self._notice_attrs[field_def['type']]

        return values_mapping
