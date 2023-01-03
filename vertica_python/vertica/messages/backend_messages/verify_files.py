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
VerifyFiles message

VerifyFiles message is sent by the server when the client issues
a COPY FROM LOCAL command. The server parses the file names out
of the command, and sends them back to the client in this message.
The client has to verify that these files exist and are readable
before running the copy.
"""

from __future__ import print_function, division, absolute_import

from struct import unpack_from

from ..message import BackendMessage


class VerifyFiles(BackendMessage):
    message_id = b'F'

    def __init__(self, data):
        BackendMessage.__init__(self)
        files_count = unpack_from('!H', data, 0)[0]
        self.input_files = [None] * files_count
        pos = 2
        for i in range(files_count):
            filename = unpack_from("!{0}sx".format(data.find(b'\x00', pos) - pos), data, pos)[0]
            self.input_files[i] = filename.decode('utf-8')
            pos += len(filename) + 1

        filename = unpack_from("!{0}sx".format(data.find(b'\x00', pos) - pos), data, pos)[0]
        self.rejections_file = filename.decode('utf-8')
        pos += len(filename) + 1

        filename = unpack_from("!{0}sx".format(data.find(b'\x00', pos) - pos), data, pos)[0]
        self.exceptions_file = filename.decode('utf-8')

    def __str__(self):
        return "VerifyFiles: InputFiles = {}, RejectedDataFile = {}, ExceptionsFile = {}".format(
               self.input_files, self.rejections_file, self.exceptions_file)


BackendMessage.register(VerifyFiles)
