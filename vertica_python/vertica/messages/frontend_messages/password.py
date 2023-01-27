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

from __future__ import print_function, division, absolute_import

import os
import hashlib
from struct import pack

from ..message import BulkFrontendMessage
from ..backend_messages.authentication import Authentication
from ....compat import as_bytes

from . import crypt_windows as crypt


class Password(BulkFrontendMessage):
    message_id = b'p'

    def __init__(self, password, auth_method=None, options=None):
        BulkFrontendMessage.__init__(self)

        self._password = as_bytes(password)
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
        elif self._auth_method in (Authentication.MD5_PASSWORD,
                                   Authentication.HASH,
                                   Authentication.HASH_MD5,
                                   Authentication.HASH_SHA512):
            # Encodes user/password/salt information in the following way:
            #   MD5(MD5(password + user) + salt)
            #   SHA512(SHA512(password + userSalt) + salt)
            useMD5 = self._auth_method in (Authentication.MD5_PASSWORD, Authentication.HASH_MD5)
            user = self._options['user'].encode('utf-8') if useMD5 else self._options['usersalt']
            for key in (user, self._options['salt']):
                m = hashlib.md5() if useMD5 else hashlib.sha512()
                m.update(self._password + key)
                hexdigest = m.hexdigest()
                self._password = hexdigest.encode('utf-8')
            prefix = b'md5' if useMD5 else b'sha512'
            return prefix + self._password
        elif self._auth_method == Authentication.GSS:
            return self._password
        else:
            raise ValueError("unsupported authentication method: {0}".format(self._auth_method))

    def read_bytes(self):
        encoded_pw = self.encoded_password()
        # Vertica server handles GSS messages differently from other passwords
        if self._auth_method == Authentication.GSS:
            bytes_ = pack('{0}s'.format(len(encoded_pw)), encoded_pw)
        else:
            bytes_ = pack('{0}sx'.format(len(encoded_pw)), encoded_pw)
        return bytes_
