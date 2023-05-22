# Copyright (c) 2023 Open Text.
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

from __future__ import print_function, division, absolute_import

import socket
import ssl

from ... import errors
from .base import VerticaPythonIntegrationTestCase


class TlsTestCase(VerticaPythonIntegrationTestCase):
    def setUp(self):
        super(TlsTestCase, self).setUp()
        print("\n",self._conn_info)

    def tearDown(self):
        if 'ssl' in self._conn_info:
            del self._conn_info['ssl']
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("ALTER TLS CONFIGURATION server CERTIFICATE NULL TLSMODE 'DISABLE'")
            cur.execute("DROP KEY IF EXISTS vp_server_key CASCADE")
            cur.execute("DROP KEY IF EXISTS vp_CA_key CASCADE")
        super(TlsTestCase, self).tearDown()

    def _generate_and_set_certificates(self):
        with self._connect() as conn:
            cur = conn.cursor()
            # Generate a root CA private key
            cur.execute("CREATE KEY vp_CA_key TYPE 'RSA' LENGTH 2048")
            # Generate a root CA certificate
            cur.execute("CREATE CA CERTIFICATE vp_CA_cert "
                    "SUBJECT '/C=US/ST=Massachusetts/L=Burlington/O=OpenText/OU=Vertica/CN=Vertica Root CA' "
                    "VALID FOR 3650 EXTENSIONS 'nsComment' = 'Self-signed root CA cert' KEY vp_CA_key")
            cur.execute("SELECT certificate_text FROM CERTIFICATES WHERE name='vp_CA_cert'")
            vp_CA_cert = cur.fetchone()[0]

            # Generate a server private key
            cur.execute("CREATE KEY vp_server_key TYPE 'RSA' LENGTH 2048")
            # Generate a server certificate
            cur.execute("CREATE CERTIFICATE vp_server_cert "
                    "SUBJECT '/C=US/ST=MA/L=Cambridge/O=Foo/OU=Vertica/CN=Vertica server/emailAddress=abc@example.com' "
                    "SIGNED BY vp_CA_cert EXTENSIONS 'nsComment' = 'Vertica server cert', 'extendedKeyUsage' = 'serverAuth', "
                    "'subjectAltName' = 'DNS:localhost' KEY vp_server_key")

            # In order to use Server Mode, set the server certificate for the server's TLS Configuration
            cur.execute('ALTER TLS CONFIGURATION server CERTIFICATE vp_server_cert')
            # Enable TLS. Server does not check client certificates.
            cur.execute("ALTER TLS CONFIGURATION server TLSMODE 'ENABLE'")

            return vp_CA_cert


    def test_TLSMode_disable(self):
        self._conn_info['ssl'] = False
        with self._connect() as conn:
            cur = conn.cursor()
            res = self._query_and_fetchone('SELECT ssl_state FROM sessions WHERE session_id=(SELECT current_session()) ')
            self.assertEqual(res[0], 'None')

    def test_TLSMode_require_server_disable(self):
        # Requires that the server use TLS. If the TLS connection attempt fails, the client rejects the connection.
        self._conn_info['ssl'] = True
        self.assertConnectionFail(err_type=errors.SSLNotSupported,
                err_msg='SSL requested but not supported by server')

    def test_TLSMode_require(self):
        # Setting certificates with TLS configuration
        self._generate_and_set_certificates()

        # Option 1
        self._conn_info['ssl'] = True
        with self._connect() as conn:
            cur = conn.cursor()
            res = self._query_and_fetchone('SELECT ssl_state FROM sessions WHERE session_id=(SELECT current_session())')
            self.assertEqual(res[0], 'Server')

        # Option 2
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        self._conn_info['ssl'] = ssl_context
        with self._connect() as conn:
            cur = conn.cursor()
            res = self._query_and_fetchone('SELECT ssl_state FROM sessions WHERE session_id=(SELECT current_session())')
            self.assertEqual(res[0], 'Server')


    def test_TLSMode_verify_ca(self):
        # Setting certificates with TLS configuration
        CA_cert = self._generate_and_set_certificates()

        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.check_hostname = False
        ssl_context.load_verify_locations(cadata=CA_cert) # CA certificate used to verify server certificate
        self._conn_info['ssl'] = ssl_context
        with self._connect() as conn:
            cur = conn.cursor()
            res = self._query_and_fetchone('SELECT ssl_state FROM sessions WHERE session_id=(SELECT current_session())')
            self.assertEqual(res[0], 'Server')

    def test_TLSMode_verify_full(self):
        # Setting certificates with TLS configuration
        CA_cert = self._generate_and_set_certificates()

        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.check_hostname = True  # hostname in server cert's subjectAltName: localhost
        ssl_context.load_verify_locations(cadata=CA_cert) # CA certificate used to verify server certificate
        self._conn_info['ssl'] = ssl_context
        with self._connect() as conn:
            cur = conn.cursor()
            res = self._query_and_fetchone('SELECT ssl_state FROM sessions WHERE session_id=(SELECT current_session())')
            self.assertEqual(res[0], 'Server')

    def test_mutual_TLS(self):
        return
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute()

            # In order to use Mutual Mode, set a server and CA certificate.
            # This CA certificate is used to verify client certificates
            cur.execute('ALTER TLS CONFIGURATION server CERTIFICATE vp_server_cert ADD CA CERTIFICATES ca_cert')
            # Enable TLS. Connection succeeds if Vertica verifies that the client certificate is from a trusted CA.
            # If the client does not present a client certificate, the connection uses plaintext.
            cur.execute("ALTER TLS CONFIGURATION server TLSMODE 'VERIFY_CA'")

        self._conn_info['ssl'] = True
        with self._connect() as conn:
            cur = conn.cursor()
            res = self._query_and_fetchone('SELECT ssl_state FROM sessions WHERE session_id=(SELECT current_session())')
            self.assertEqual(res[0], 'Mutual')


exec(TlsTestCase.createPrepStmtClass())
