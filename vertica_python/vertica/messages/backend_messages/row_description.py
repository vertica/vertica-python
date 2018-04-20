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

from struct import unpack, unpack_from

from six.moves import range

from ..message import BackendMessage


class RowDescription(BackendMessage):
    message_id = b'T'

    def __init__(self, data):
        BackendMessage.__init__(self)
        self.fields = []
        field_count = unpack('!H', data[0:2])[0]
        pos = 2

        for i in range(field_count):
            field_info = unpack_from("!{0}sxIHIHIH".format(data.find(b'\x00', pos) - pos), data,
                                     pos)
            self.fields.append({
                'name': field_info[0],
                'table_oid': field_info[1],
                'attribute_number': field_info[2],
                'data_type_oid': field_info[3],
                'data_type_size': field_info[4],
                'type_modifier': field_info[5],
                'format_code': field_info[6],
            })

            pos += 19 + len(field_info[0])


BackendMessage.register(RowDescription)
