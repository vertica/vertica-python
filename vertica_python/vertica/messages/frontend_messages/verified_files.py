# Copyright (c) 2020-2024 Open Text.
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


from __future__ import print_function, division, absolute_import, annotations

import os
from struct import pack

from ..message import BulkFrontendMessage


class VerifiedFiles(BulkFrontendMessage):
    message_id = b'F'

    def __init__(self, file_list, protocol_version):
        BulkFrontendMessage.__init__(self)
        self.filenames = file_list
        self.protocol_version = protocol_version

    def read_bytes(self):
        if self.protocol_version >= (3 << 16 | 15):
            bytes_ = pack('!I', len(self.filenames)) # Int32
        else:
            bytes_ = pack('!H', len(self.filenames)) # Int16
        for filename in self.filenames:
            utf_filename = filename.encode('utf-8')
            bytes_ += pack('!{0}sx'.format(len(utf_filename)), utf_filename)
            bytes_ += pack('!Q', os.path.getsize(filename))

        return bytes_

    def __str__(self):
        return "VerifiedFiles: {}".format(self.filenames)
