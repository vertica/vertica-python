# Copyright (c) 2020-2023 Micro Focus or one of its affiliates.
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


from __future__ import print_function, division, absolute_import

from struct import unpack

from ..message import BackendMessage


class LoadFile(BackendMessage):
    message_id = b'H'

    def __init__(self, data):
        BackendMessage.__init__(self)
        unpacked = unpack('!{0}sx'.format(data.find(b'\x00')), data)
        self.filename = unpacked[0].decode('utf-8')

    def __str__(self):
        return "LoadFile: name = {}".format(self.filename)


BackendMessage.register(LoadFile)
