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

from struct import unpack

from ..message import BackendMessage
from .... import errors


class Authentication(BackendMessage):
    message_id = b'R'

    OK = 0
    KERBEROS_V4 = 1
    KERBEROS_V5 = 2
    CLEARTEXT_PASSWORD = 3
    CRYPT_PASSWORD = 4  # obsolete
    MD5_PASSWORD = 5
    SCM_CREDENTIAL = 6
    GSS = 7
    GSS_CONTINUE = 8
    CHANGE_PASSWORD = 9
    PASSWORD_CHANGED = 10  # client doesn't do password changing, this should never be seen
    PASSWORD_GRACE = 11
    OAUTH = 12
    HASH = 65536
    HASH_MD5 = 65536 + 5
    HASH_SHA512 = 65536 + 512

    def __init__(self, data):
        BackendMessage.__init__(self)
        self.code, other = unpack('!I{0}s'.format(len(data) - 4), data)

        if self.code == self.CRYPT_PASSWORD:
            self.salt = other
        elif self.code in (self.MD5_PASSWORD, self.HASH_MD5):
            self.salt = other[:4]
        elif self.code in (self.HASH, self.HASH_SHA512):
            self.salt = other[:4]
            userSaltLen = unpack('!I', other[4:8])[0]
            if userSaltLen != 16:
                raise errors.MessageError(
                    'Received wrong user salt size: {}'.format(userSaltLen))
            self.usersalt = unpack('!{0}s'.format(userSaltLen), other[8:])[0]
        elif self.code in [self.GSS_CONTINUE]:
            self.auth_data = other

    def __str__(self):
        return "Authentication: type={}".format(self.code)


BackendMessage.register(Authentication)
