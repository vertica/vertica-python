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
Startup message

To begin a session, the frontend opens a connection to the backend and sends a
Startup message.
"""

from __future__ import print_function, division, absolute_import

import platform
import os
from struct import pack

# noinspection PyUnresolvedReferences,PyCompatibility

import vertica_python
from ..message import BulkFrontendMessage


class Startup(BulkFrontendMessage):
    message_id = None

    def __init__(self, user, database, session_label, os_user_name, autocommit,
                 binary_transfer, request_complex_types):
        BulkFrontendMessage.__init__(self)

        try:
            os_platform = platform.platform()
        except Exception as e:
            os_platform = ''
            print("WARN: Cannot get the OS info: {}".format(str(e)))

        try:
            pid = str(os.getpid())
        except Exception as e:
            pid = '0'
            print("WARN: Cannot get the process ID: {}".format(str(e)))

        request_complex_types = 'true' if request_complex_types else 'false'

        self.parameters = {
            b'user': user,
            b'database': database,
            b'client_label': session_label,
            b'client_type': 'vertica-python',
            b'client_version': vertica_python.__version__,
            b'client_os': os_platform,
            b'client_os_user_name': os_user_name,
            b'client_pid': pid,
            b'autocommit': 'on' if autocommit else 'off',
            b'binary_data_protocol': '1' if binary_transfer else '0', # Defaults to text format '0'
            b'protocol_features': '{"request_complex_types":' + request_complex_types + '}',
        }

    def read_bytes(self):
        # The fixed protocol version is followed by pairs of parameter name and value strings.
        # A zero byte is required as a terminator after the last name/value pair.
        # Parameters can appear in any order.
        fixed_protocol_version = 3 << 16 | 5
        bytes_ = pack('!I', fixed_protocol_version)

        # The frontend sends a requested protocol version to the backend.
        # Old servers (protocol < 3.7) ignore this value and use the fixed protocol version.
        # New servers (protocol >= 3.7) would try to find the common protocol
        # version in use for both client and server, and send back a ParameterStatus
        # message (key='protocol_version', value=<effective protocol version>)
        bytes_ += pack('!16sxIx', b'protocol_version', vertica_python.PROTOCOL_VERSION)

        for k in self.parameters:
            v = self.parameters[k].encode('utf-8')
            bytes_ += pack('!{}sx{}sx'.format(len(k), len(v)), k, v)

        bytes_ += pack('x')
        return bytes_
