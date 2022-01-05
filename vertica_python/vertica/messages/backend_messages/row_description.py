# Copyright (c) 2018-2022 Micro Focus or one of its affiliates.
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

"""
RowDescription message

RowDescription message describes the column layout of the rows that will be
returned in response to a SELECT, FETCH, etc query.
"""

from __future__ import print_function, division, absolute_import

from struct import unpack, unpack_from, calcsize

from six.moves import range

from ..message import BackendMessage
from ....datatypes import getTypeName


class RowDescription(BackendMessage):
    message_id = b'T'

    def __init__(self, data):
        BackendMessage.__init__(self)
        self.fields = []
        field_count = unpack('!H', data[0:2])[0]

        if field_count == 0:
            return

        # read type pool
        # used for special types e.g. GEOMETRY, GEOGRAPHY
        user_types = []
        type_pool_count = unpack('!I', data[2:6])[0]
        pos = 6
        for _ in range(type_pool_count):
            base_type_oid = unpack('!I', data[pos:(pos + 4)])[0]
            pos += 4
            type_name = unpack_from("!{0}sx".format(data.find(b'\x00', pos) - pos), data, pos)[0]
            pos += len(type_name) + 1
            user_types.append((base_type_oid, type_name.decode('utf-8')))

        # read info of each field
        offset = calcsize("!HBIHHHiH")
        for _ in range(field_count):
            field_name = unpack_from("!{0}sx".format(data.find(b'\x00', pos) - pos), data, pos)[0]
            pos += len(field_name) + 1
            field_name = field_name.decode('utf-8')

            table_oid = unpack('!Q', data[pos:(pos + 8)])[0]
            pos += 8

            schema_name, table_name = None, None
            if table_oid != 0:
                schema_name = unpack_from("!{0}sx".format(data.find(b'\x00', pos) - pos), data, pos)[0]
                pos += len(schema_name) + 1
                schema_name = schema_name.decode('utf-8')
                table_name = unpack_from("!{0}sx".format(data.find(b'\x00', pos) - pos), data, pos)[0]
                pos += len(table_name) + 1
                table_name = table_name.decode('utf-8')

            field_info = unpack_from("!HBIhHHiH", data, pos)
            pos += offset

            if field_info[1] == 1:
                data_type_oid, data_type_name = user_types[field_info[2]]
            else:
                data_type_oid = field_info[2]
                data_type_name = getTypeName(data_type_oid, field_info[6])

            self.fields.append({
                'name': field_name,
                'table_oid': table_oid,
                'schema_name': schema_name,
                'table_name': table_name,
                'attribute_number': field_info[0],
                'data_type_oid': data_type_oid,
                'data_type_size': field_info[3],
                'data_type_name': data_type_name,
                'null_ok': field_info[4] == 1,
                'is_identity': field_info[5] == 1,
                'type_modifier': field_info[6],
                'format_code': field_info[7],
            })

    def __str__(self):
        return "RowDescription: {}".format(self.fields)


BackendMessage.register(RowDescription)
