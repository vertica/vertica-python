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


class Execute(BulkFrontendMessage):
    message_id = b'E'

    def __init__(self, portal_name, max_rows):
        BulkFrontendMessage.__init__(self)
        self._portal_name = portal_name
        self._max_rows = max_rows

    def read_bytes(self):
        bytes_ = pack('!{0}sxI'.format(len(self._portal_name)), self._portal_name, self._max_rows)
        return bytes_
