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

import os
import hashlib
from struct import pack

import six

from ..message import BulkFrontendMessage
from ..backend_messages.authentication import Authentication

if os.name == 'nt':
    from . import crypt_windows as crypt
else:
    import crypt

ASCII = 'ascii'


class Password(BulkFrontendMessage):
    message_id = b'p'

    def __init__(self, password, auth_method=None, options=None):
        BulkFrontendMessage.__init__(self)

        self._password = password
        self._options = options or {}
        if auth_method is not None:
            self._auth_method = auth_method
        else:
            self._auth_method = Authentication.CLEARTEXT_PASSWORD

    def encoded_password(self):

        if self._auth_method == Authentication.CLEARTEXT_PASSWORD:
            return self._password
        elif self._auth_method == Authentication.CRYPT_PASSWORD:
            return crypt.crypt(self._password, self._options['salt'])
        elif self._auth_method == Authentication.MD5_PASSWORD:
            for key in 'user', 'salt':
                m = hashlib.md5()
                m.update(self._password + self._options[key])
                hexdigest = m.hexdigest()
                if six.PY3:
                    # In python3 the output of m.hexdigest() is a unicode string,
                    # so has to be converted to bytes before concat'ing with
                    # the password bytes.
                    hexdigest = bytes(hexdigest, ASCII)
                self._password = hexdigest

            prefix = 'md5'
            if six.PY3:
                # Same workaround for bytes here.
                prefix = bytes(prefix, ASCII)
            return prefix + self._password
        else:
            raise ValueError("unsupported authentication method: {0}".format(self._auth_method))

    def read_bytes(self):
        encoded_pw = self.encoded_password()
        bytes_ = pack('{0}sx'.format(len(encoded_pw)), encoded_pw)
        return bytes_
