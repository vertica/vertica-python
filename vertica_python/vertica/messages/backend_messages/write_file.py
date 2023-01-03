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


"""
WriteFile message

In the copy-local protocol, the server may send a WriteFile message when it
receives a batch of copy data. If the COPY FROM LOCAL command uses the
REJECTED DATA and/or EXCEPTIONS parameters, this message contains content of
rejected rows or exceptions output files. If the command uses the
RETURNREJECTED parameters instead, this message is a series of row numbers
saying which rows in the load were rejected.
"""

from __future__ import print_function, division, absolute_import

from struct import unpack_from

from ..message import BackendMessage


class WriteFile(BackendMessage):
    message_id = b'O'

    def __init__(self, filename, file_length, data=None):
        BackendMessage.__init__(self)
        self.filename = filename
        self.file_length = file_length

        # Parse RETURNREJECTED data
        if self.filename == '':
            row_count = self.file_length // 8
            # Rejected row numbers come in little endian format
            self.rejected_rows = unpack_from('<{0}Q'.format(row_count), data)

    def write_to_disk(self, connection, buffer_size):
        # Read the rest of the message from wire and write the file
        bytes_left = self.file_length
        with open(self.filename, 'ab') as f:
            pos = 0
            while bytes_left > 0:
                bytes_to_read = min(buffer_size, bytes_left)
                content = connection.read_bytes(bytes_to_read)
                f.write(content)
                bytes_left -= bytes_to_read

    def __str__(self):
        return "WriteFile: Filename = {}, FileLength = {}".format(self.filename, self.file_length)


BackendMessage.register(WriteFile)
