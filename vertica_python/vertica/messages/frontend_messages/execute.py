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
Execute message

In the extended query protocol, the frontend sends a Execute message once a
portal exists. Execute doesn't cause RowDescription response message to be
issued, so the frontend should issue Describe before issuing Execute, to ensure
that it knows how to interpret the result rows it will get back.

The Execute message specifies the portal name and a maximum result-row count.
Currently, Vertica backend will ignore this result-row count and send all the
rows regardless of what you put here.
"""

from __future__ import print_function, division, absolute_import

from struct import pack

from ..message import BulkFrontendMessage


class Execute(BulkFrontendMessage):
    message_id = b'E'

    def __init__(self, portal_name, max_rows):
        BulkFrontendMessage.__init__(self)
        self._portal_name = portal_name
        self._max_rows = max_rows

    def read_bytes(self):
        utf_portal_name = self._portal_name.encode('utf-8')
        bytes_ = pack('!{0}sxI'.format(len(utf_portal_name)), utf_portal_name, self._max_rows)
        return bytes_
