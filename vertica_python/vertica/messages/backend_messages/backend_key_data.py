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

from struct import unpack

from ..message import BackendMessage


class BackendKeyData(BackendMessage):
    message_id = b'K'

    def __init__(self, data):
        BackendMessage.__init__(self)
        unpacked = unpack('!2I', data)
        self.pid = unpacked[0]
        self.key = unpacked[1]


BackendMessage.register(BackendKeyData)
