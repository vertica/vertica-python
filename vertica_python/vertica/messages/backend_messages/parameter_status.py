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
ParameterStatus message

A ParameterStatus message will be generated whenever the backend believes the
frontend should know about a setting parameter value. For example, when you do
SET SESSION AUTOCOMMIT ON | OFF, you get back a parameter status telling you the
new value of autocommit.

At present Vertica supports a handful of parameters, they are:
  standard_conforming_strings, server_version, client_locale, client_label,
  long_string_types, protocol_version, auto_commit, MARS

More parameters would be added in the future. Accordingly, a frontend should
simply ignore ParameterStatus for parameters that it does not understand or care
about.
"""

from __future__ import print_function, division, absolute_import

from struct import unpack

from ..message import BackendMessage


class ParameterStatus(BackendMessage):
    message_id = b'S'

    def __init__(self, data):
        BackendMessage.__init__(self)
        null_byte = data.find(b'\x00')
        unpacked = unpack('{0}sx{1}sx'.format(null_byte, len(data) - null_byte - 2), data)
        self.name = unpacked[0].decode('utf-8')
        self.value = unpacked[1].decode('utf-8')

    def __str__(self):
        return "ParameterStatus: {} = {}".format(self.name, self.value)


BackendMessage.register(ParameterStatus)
