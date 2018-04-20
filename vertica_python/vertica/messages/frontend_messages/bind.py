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


class Bind(BulkFrontendMessage):
    message_id = b'B'

    def __init__(self, portal_name, prepared_statement_name, parameter_values):
        BulkFrontendMessage.__init__(self)
        self._portal_name = portal_name
        self._prepared_statement_name = prepared_statement_name
        self._parameter_values = parameter_values

    def read_bytes(self):
        bytes_ = pack('!{0}sx{1}sxHH'.format(
            len(self._portal_name), len(self._prepared_statement_name)),
            self._portal_name, self._prepared_statement_name, 0, len(self._parameter_values))

        for val in self._parameter_values.values():
            if val is None:
                bytes_ += pack('!I', [-1])
            else:
                bytes_ += pack('!I{0}s'.format(len(val)), len(val), val)
        bytes_ += pack('!H', [0])

        return bytes_
