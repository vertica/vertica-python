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

UTF_8 = 'utf-8'


class Query(BulkFrontendMessage):
    message_id = b'Q'

    def __init__(self, query_string):
        BulkFrontendMessage.__init__(self)
        self._query_string = query_string

    def read_bytes(self):
        encoded = self._query_string.encode(UTF_8)
        bytes_ = pack('{0}sx'.format(len(encoded)), encoded)
        return bytes_
