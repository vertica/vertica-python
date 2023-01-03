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
Parse message

In the extended query protocol, the frontend first sends a Parse message, which
contains a textual query string. The query string leaves certain values
unspecified with parameter placeholders (i.e. question mark '?').

The response is either ParseComplete or ErrorResponse. The query string cannot
include more than one SQL statement; else an ErrorResponse is reported. The
error message would be something like
  "Cannot insert multiple commands into a prepared statement"
"""

from __future__ import print_function, division, absolute_import

from struct import pack

from ..message import BulkFrontendMessage


class Parse(BulkFrontendMessage):
    message_id = b'P'

    def __init__(self, name, query, param_types):
        BulkFrontendMessage.__init__(self)

        self._name = name
        self._query = query
        self._param_types = param_types

    def read_bytes(self):
        utf_name = self._name.encode('utf-8')
        utf_query = self._query.encode('utf-8')

        bytes_ = pack('!{0}sx{1}sxH'.format(len(utf_name), len(utf_query)),
                      utf_name, utf_query, len(self._param_types))

        for param in self._param_types:
            bytes_ += pack('!I', param)

        return bytes_
