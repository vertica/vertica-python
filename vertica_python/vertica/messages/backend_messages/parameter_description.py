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

from struct import unpack, unpack_from, calcsize

from ..message import BackendMessage
from ....datatypes import getTypeName


class ParameterDescription(BackendMessage):
    message_id = b't'

    def __init__(self, data):
        BackendMessage.__init__(self)
        self.parameters = []
        self.parameter_count = unpack('!H', data[0:2])[0]
        if self.parameter_count == 0:
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
            user_types.append((base_type_oid, type_name))

        # read info of each parameter
        offset = calcsize("!BIiH")
        for _ in range(self.parameter_count):
            field_info = unpack_from("!BIiH", data, pos)
            pos += offset

            if field_info[0] == 1:
                data_type_oid, data_type_name = user_types[field_info[1]]
            else:
                data_type_oid = field_info[1]
                data_type_name = getTypeName(data_type_oid, field_info[2])

            self.parameters.append({
                'data_type_oid': data_type_oid,
                'data_type_name': data_type_name,
                'type_modifier': field_info[2],
                'null_ok': field_info[3] != 1,
            })

    def __str__(self):
        return "ParameterDescription: {}".format(self.parameters)


BackendMessage.register(ParameterDescription)
