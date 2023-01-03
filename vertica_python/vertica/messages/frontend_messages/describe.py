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
Describe message

In the extended query protocol, the frontend sends a Describe message, which
specifies the name of an existing prepared statement.

The first response is a ParameterDescription message describing the parameters
needed by the statement. The second response is a RowDescription message
describing the rows that will be returned when the statement is eventually
executed (or a NoData message if the statement will not return rows). The third
response is a CommandDescription message describing the type of command to be
executed and any semantically-equivalent COPY statement.
"""

from __future__ import print_function, division, absolute_import

from struct import pack

from ..message import BulkFrontendMessage


class Describe(BulkFrontendMessage):
    message_id = b'D'

    def __init__(self, describe_type, describe_name):
        BulkFrontendMessage.__init__(self)

        self._describe_name = describe_name

        if describe_type == 'portal':
            self._describe_type = b'P'
        elif describe_type == 'prepared_statement':
            self._describe_type = b'S'
        else:
            raise ValueError("{0} is not a valid describe_type. "
                             "Must be either portal or prepared_statement".format(describe_type))

    def read_bytes(self):
        utf_name = self._describe_name.encode('utf-8')
        bytes_ = pack('c{0}sx'.format(len(utf_name)), self._describe_type, utf_name)
        return bytes_

    def __str__(self):
        return 'Describe: type = {}, name = "{}"'.format(
               'Portal' if self._describe_type == b'P' else 'Prepared Statement' , self._describe_name)
