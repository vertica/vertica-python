# Copyright (c) 2024 Open Text.
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


from __future__ import print_function, division, absolute_import, annotations

import ssl
from enum import Enum

class TLSMode(Enum):
    DISABLE = 'disable'
    PREFER = 'prefer'
    REQUIRE = 'require'
    VERIFY_CA = 'verify-ca'
    VERIFY_FULL = 'verify-full'

    def requests_encryption(self) -> bool:
        return self != TLSMode.DISABLE

    def requires_encryption(self) -> bool:
        return self not in (TLSMode.DISABLE, TLSMode.PREFER)

    def verify_certificate(self) -> bool:
        return self in (TLSMode.VERIFY_CA, TLSMode.VERIFY_FULL)

    def verify_hostname(self) -> bool:
        return self == TLSMode.VERIFY_FULL

    def get_sslcontext(self, cafile=None) -> ssl.SSLContext:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.check_hostname = self.verify_hostname()
        ssl_context.verify_mode = ssl.CERT_REQUIRED if self.verify_certificate() else ssl.CERT_NONE
        if cafile:
            ssl_context.load_verify_locations(cafile=cafile)
        return ssl_context

