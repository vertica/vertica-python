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

from collections import namedtuple

from ..datatypes import getDisplaySize, getPrecision, getScale
from ..compat import as_str, as_text


# Data of a particular SQL data type might be transmitted in either "text" format or "binary" format.
# The desired format for any column is specified by a format code.
class FormatCode(object):
    TEXT = 0
    BINARY = 1


ColumnTuple = namedtuple('Column', ['name', 'type_code', 'display_size', 'internal_size',
                                    'precision', 'scale', 'null_ok'])


class Column(object):
    def __init__(self, col):
        # Describe one query result column
        self.name = col['name']
        self.type_code = col['data_type_oid']
        self.type_name = col['data_type_name']
        self.table_oid = col['table_oid']
        self.schema_name = col['schema_name']
        self.table_name = col['table_name']
        self.attribute_number = col['attribute_number']
        self.display_size = getDisplaySize(col['data_type_oid'], col['type_modifier'])
        self.internal_size = col['data_type_size']
        self.precision = getPrecision(col['data_type_oid'], col['type_modifier'])
        self.scale = getScale(col['data_type_oid'], col['type_modifier'])
        self.null_ok = col['null_ok']
        self.is_identity = col['is_identity']
        self.format_code = col['format_code']
        self.child_columns = None
        self.props = ColumnTuple(self.name, self.type_code, self.display_size, self.internal_size,
                                 self.precision, self.scale, self.null_ok)

    def add_child_column(self, col):
        """
        Complex types involve multiple columns arranged in a hierarchy of parents and children.
        Each parent column stores references to child columns in a list.
        """
        if self.child_columns is None:
            self.child_columns = []
        self.child_columns.append(col)

    def __str__(self):
        return as_str(str(self.props))

    def __unicode__(self):
        return as_text(str(self.props))

    def __repr__(self):
        return as_str(str(self.props))

    def __iter__(self):
        for prop in self.props:
            yield prop

    def __getitem__(self, key):
        return self.props[key]

    def __len__(self):
        return len(self.props)
