# Copyright (c) 2013-2017 Uber Technologies, Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
