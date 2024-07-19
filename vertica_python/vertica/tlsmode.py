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
import warnings
from enum import Enum
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from typing import Optional


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

    def get_sslcontext(self, cafile: Optional[str] = None,
                       certfile: Optional[str] = None,
                       keyfile: Optional[str] = None) -> ssl.SSLContext:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.check_hostname = self.verify_hostname()
        if self.verify_certificate():
            ssl_context.verify_mode = ssl.CERT_REQUIRED
            if cafile:
                ssl_context.load_verify_locations(cafile=cafile)
            # mutual mode
            if certfile or keyfile:
                ssl_context.load_cert_chain(certfile=certfile, keyfile=keyfile)
        else:
            ssl_context.verify_mode = ssl.CERT_NONE
            if cafile or certfile or keyfile:
                ignore_cert_msg = ("Ignore TLS certificate files and skip certificates"
                        f" validation as tlsmode is not '{TLSMode.VERIFY_CA.value}'"
                        f" or '{TLSMode.VERIFY_FULL.value}'.")
                warnings.warn(ignore_cert_msg)
        return ssl_context

