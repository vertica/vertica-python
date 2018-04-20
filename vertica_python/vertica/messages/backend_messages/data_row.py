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


class DataRow(BackendMessage):
    message_id = b'D'

    def __init__(self, data):
        BackendMessage.__init__(self)
        self.values = []
        field_count = unpack('!H', data[0:2])[0]
        pos = 2

        for i in range(field_count):
            size = unpack_from('!I', data, pos)[0]

            if size == 4294967295:
                size = -1

            if size == -1:
                self.values.append(None)
            else:
                self.values.append(unpack_from('{0}s'.format(size), data, pos + 4)[0])
            pos += (4 + max(size, 0))


BackendMessage.register(DataRow)
