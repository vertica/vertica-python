from __future__ import print_function, division, absolute_import

from struct import unpack

from ..message import BackendMessage


class Authentication(BackendMessage):
    message_id = b'R'

    OK = 0
    KERBEROS_V5 = 2
    CLEARTEXT_PASSWORD = 3
    CRYPT_PASSWORD = 4
    MD5_PASSWORD = 5
    SCM_CREDENTIAL = 6
    GSS = 7
    GSS_CONTINUE = 8
    SSPI = 9

    def __init__(self, data):
        BackendMessage.__init__(self)
        unpacked = unpack('!I{0}s'.format(len(data) - 4), data)
        self.code = unpacked[0]
        other = unpacked[1::][0]
        if self.code in [self.CRYPT_PASSWORD, self.MD5_PASSWORD]:
            self.salt = other
        if self.code in [self.GSS_CONTINUE]:
            self.auth_data = other


BackendMessage.register(Authentication)
