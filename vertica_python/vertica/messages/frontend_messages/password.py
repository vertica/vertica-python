from __future__ import print_function, division, absolute_import

import os
import hashlib
from struct import pack

import six

from ..message import FrontendMessage
from ..backend_messages.authentication import Authentication

if os.name == 'nt':
    from . import crypt_windows as crypt
else:
    import crypt

ASCII = 'ascii'


class Password(FrontendMessage):
    message_id = b'p'

    def __init__(self, password, auth_method=None, options=None):
        FrontendMessage.__init__(self)

        self.password = password
        self.options = options or {}
        if auth_method is not None:
            self.auth_method = auth_method
        else:
            self.auth_method = Authentication.CLEARTEXT_PASSWORD

    def encoded_password(self):

        if self.auth_method == Authentication.CLEARTEXT_PASSWORD:
            return self.password
        elif self.auth_method == Authentication.CRYPT_PASSWORD:
            return crypt.crypt(self.password, self.options['salt'])
        elif self.auth_method == Authentication.MD5_PASSWORD:
            for key in 'user', 'salt':
                m = hashlib.md5()
                m.update(self.password + self.options[key])
                hexdigest = m.hexdigest()
                if six.PY3:
                    # In python3 the output of m.hexdigest() is a unicode string,
                    # so has to be converted to bytes before concat'ing with
                    # the password bytes.
                    hexdigest = bytes(hexdigest, ASCII)
                self.password = hexdigest

            prefix = 'md5'
            if six.PY3:
                # Same workaround for bytes here.
                prefix = bytes(prefix, ASCII)
            return prefix + self.password
        else:
            raise ValueError("unsupported authentication method: {0}".format(self.auth_method))

    def to_bytes(self):
        encoded_pw = self.encoded_password()
        return self.message_string(pack('{0}sx'.format(len(encoded_pw)), encoded_pw))
