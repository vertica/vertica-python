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
Bind message

In the extended query protocol, the frontend sends a Bind message to bind values
to parameter placeholders present in an existing prepared statement.

The response is either BindComplete or ErrorResponse.
"""

from __future__ import print_function, division, absolute_import

from struct import pack

from ..message import BulkFrontendMessage
from ....datatypes import VerticaType
from ....compat import as_bytes

BACKSLASH = b'\\'
BACKSLASH_ESCAPE = b'\\134'


class Bind(BulkFrontendMessage):
    message_id = b'B'

    def __init__(self, portal_name, prepared_statement_name, parameter_values,
                 parameter_type_oids, binary_transfer):
        BulkFrontendMessage.__init__(self)
        self._portal_name = portal_name
        self._prepared_statement_name = prepared_statement_name
        self._parameter_values = parameter_values
        self._parameter_type_oids = parameter_type_oids
        self._binary_transfer = binary_transfer

    def read_bytes(self):
        utf_portal_name = self._portal_name.encode('utf-8')
        utf_prepared_statement_name = self._prepared_statement_name.encode('utf-8')

        bytes_ = pack('!{0}sx{1}sx'.format(len(utf_portal_name), len(utf_prepared_statement_name)),
                      utf_portal_name, utf_prepared_statement_name)

        # Parameter format codes -- use the default format (text)
        bytes_ += pack('!H', 0)

        # Number of parameters
        bytes_ += pack('!H', len(self._parameter_type_oids))

        param_bytes_ = b''
        for oid, val in zip(self._parameter_type_oids, self._parameter_values):
            # Parameter type oids
            bytes_ += pack('!I', oid)
            # Parameter values
            if val is None:  # -1 indicates a NULL parameter value
                param_bytes_ += pack('!i', -1)
            elif oid in (VerticaType.BINARY, VerticaType.VARBINARY, VerticaType.LONGVARBINARY):
                # Encode binary data as UTF8 bytes
                val = as_bytes(val)
                # Escape the byte value \ with "\134"(octal for backslash)
                val = val.replace(BACKSLASH, BACKSLASH_ESCAPE)
                param_bytes_ += pack('!I{0}s'.format(len(val)), len(val), val)
            else:
                # Convert input to string
                if oid == VerticaType.BOOL:
                    val = '1' if str(val).lower() in ('t', 'true', 'y', 'yes', '1') else '0'
                elif not isinstance(val, (str, bytes)):
                    val = str(val)
                # Encode string as UTF8 bytes
                val = val.encode('utf-8') if not isinstance(val, bytes) else val
                param_bytes_ += pack('!I{0}s'.format(len(val)), len(val), val)

        bytes_ += param_bytes_

        # Result column transfer format
        if self._binary_transfer:
            bytes_ += pack('!H', 1) # Specify the number of format codes followed
            bytes_ += pack('!H', 1) # Use binary format for all result columns
        else:
            # Use the default format (text) for all result columns
            bytes_ += pack('!H', 0)

        return bytes_
