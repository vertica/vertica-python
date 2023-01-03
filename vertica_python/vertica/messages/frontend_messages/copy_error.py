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
CopyError message

In the copy-local protocol, the frontend can terminate the cycle by sending a
CopyError message, which will cause the COPY SQL statement to fail with an error.
"""

from __future__ import print_function, division, absolute_import

from struct import pack

from ..message import BulkFrontendMessage


class CopyError(BulkFrontendMessage):
    message_id = b'e'

    def __init__(self, error_msg, stack_trace=None):
        BulkFrontendMessage.__init__(self)
        self.error_msg = error_msg.encode('utf-8')
        self.file_name = stack_trace[0].encode('utf-8') if stack_trace else b''
        self.line_number = stack_trace[1] if stack_trace else 0
        self.func_name = stack_trace[2].encode('utf-8') if stack_trace else b''

    def read_bytes(self):
        bytes_ = pack('!{0}sxI{1}sx{2}sx'.format(
                      len(self.file_name), len(self.func_name), len(self.error_msg)),
                      self.file_name, self.line_number, self.func_name, self.error_msg)
        return bytes_
