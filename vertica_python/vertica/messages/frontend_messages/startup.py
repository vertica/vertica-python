# Copyright (c) 2018 Micro Focus or one of its affiliates.
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

from __future__ import print_function, division, absolute_import

import platform
import os
import getpass
from struct import pack

# noinspection PyUnresolvedReferences,PyCompatibility
from builtins import str

import vertica_python
from ..message import BulkFrontendMessage

ASCII = 'ascii'


class Startup(BulkFrontendMessage):
    message_id = None

    def __init__(self, user, database, session_label, options=None):
        BulkFrontendMessage.__init__(self)

        self._user = user
        self._database = database
        self._session_label = session_label
        self._options = options
        self._type = b'vertica-python'
        self._version = vertica_python.__version__.encode(ASCII)
        self._platform = platform.platform().encode(ASCII)
        self._pid = '{0}'.format(os.getpid()).encode(ASCII)
        self._os_user_name = getpass.getuser().encode(ASCII)

    def read_bytes(self):
        bytes_ = pack('!I', vertica_python.PROTOCOL_VERSION)
        if self._user is not None:
            bytes_ += pack('4sx{0}sx'.format(len(self._user)), b'user', self._user)
        if self._database is not None:
            bytes_ += pack('8sx{0}sx'.format(len(self._database)), b'database', self._database)
        if self._options is not None:
            bytes_ += pack('7sx{0}sx'.format(len(self._options)), b'options', self._options)
        bytes_ += pack('12sx{0}sx'.format(len(self._session_label)), b'client_label', self._session_label)
        bytes_ += pack('11sx{0}sx'.format(len(self._type)), b'client_type', self._type)
        bytes_ += pack('14sx{0}sx'.format(len(self._version)), b'client_version', self._version)
        bytes_ += pack('9sx{0}sx'.format(len(self._platform)), b'client_os', self._platform)
        bytes_ += pack('19sx{0}sx'.format(len(self._os_user_name)), b'client_os_user_name', self._os_user_name)
        bytes_ += pack('10sx{0}sx'.format(len(self._pid)), b'client_pid', self._pid)
        bytes_ += pack('x')

        return bytes_
