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

from struct import pack

from ..message import BulkFrontendMessage


class Parse(BulkFrontendMessage):
    message_id = b'P'

    def __init__(self, name, query, param_types):
        BulkFrontendMessage.__init__(self)

        self._name = name
        self._query = query
        self._param_types = param_types

    def read_bytes(self):
        params = ""
        for param in self._param_types:
            params = params + param

        bytes_ = pack('!{0}sx{1}sxH{2}I'.format(len(self._name), len(self._query),
                                                len(self._param_types)),
                      self._name, self._query, len(self._param_types), params)
        return bytes_
