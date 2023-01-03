# Copyright (c) 2018-2023 Micro Focus or one of its affiliates.
# Copyright (c) 2018 Uber Technologies, Inc.
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

# Copyright (c) 2013-2017 Uber Technologies, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

"""
CommandDescription message -- part of the response to a Describe request message.

This response informs the client about the type of command being executed.
If the command is a parameterized INSERT statement, the copy_rewrite field may
include a semantically-equivalent COPY STDIN statement. Clients can choose to
run this statement instead to achieve better performance when loading many
batches of parameters.
"""

from __future__ import print_function, division, absolute_import

from struct import unpack

from ..message import BackendMessage


class CommandDescription(BackendMessage):
    message_id = b'm'

    def __init__(self, data):
        BackendMessage.__init__(self)
        pos = data.find(b'\x00')
        unpacked = unpack("!{0}sxH{1}sx".format(pos, len(data) - pos - 4), data)

        self.command_tag = unpacked[0].decode('utf-8')
        self.has_copy_rewrite = (unpacked[1] == 1)
        self.copy_rewrite = unpacked[2].decode('utf-8')

    def __str__(self):
        return ('CommandDescription: command_tag = "{}", has_copy_rewrite = {},'
                ' copy_rewrite = "{}"'.format(
                    self.command_tag, self.has_copy_rewrite, self.copy_rewrite))


BackendMessage.register(CommandDescription)
