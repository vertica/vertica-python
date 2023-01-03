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
Close message

In the extended query protocol, the frontend sends a Close message to close an
existing prepared statement or portal and release resources.

The response is either CloseComplete or ErrorResponse. It is not an error to
issue Close against a nonexistent statement or portal name.
"""

from __future__ import print_function, division, absolute_import

from struct import pack

from ..message import BulkFrontendMessage


class Close(BulkFrontendMessage):
    message_id = b'C'

    def __init__(self, close_type, close_name):
        BulkFrontendMessage.__init__(self)

        self._close_name = close_name

        if close_type == 'portal':
            self._close_type = b'P'
        elif close_type == 'prepared_statement':
            self._close_type = b'S'
        else:
            raise ValueError("{0} is not a valid close_type. "
                             "Must be either portal or prepared_statement".format(close_type))

    def read_bytes(self):
        utf_close_name = self._close_name.encode('utf-8')
        bytes_ = pack('c{0}sx'.format(len(utf_close_name)), self._close_type, utf_close_name)
        return bytes_
