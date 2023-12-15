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

from __future__ import print_function, division, absolute_import, annotations

import os
import socket
import ssl
from tempfile import NamedTemporaryFile

from ... import errors
from .base import VerticaPythonIntegrationTestCase


class TlsTestCase(VerticaPythonIntegrationTestCase):
    def tearDown(self):
        if 'ssl' in self._conn_info:
            del self._conn_info['ssl']
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("ALTER TLS CONFIGURATION server CERTIFICATE NULL TLSMODE 'DISABLE'")
            if hasattr(self, 'client_cert'):
                os.remove(self.client_cert.name)
                cur.execute("ALTER TLS CONFIGURATION server REMOVE CA CERTIFICATES vp_CA_cert")
            if hasattr(self, 'client_key'):
                os.remove(self.client_key.name)
                cur.execute("DROP KEY IF EXISTS vp_client_key CASCADE")
            cur.execute("DROP KEY IF EXISTS vp_server_key CASCADE")
            cur.execute("DROP KEY IF EXISTS vp_CA_key CASCADE")
        super(TlsTestCase, self).tearDown()

    def _generate_and_set_certificates(self, mutual_mode=False):
        with self._connect() as conn:
            cur = conn.cursor()

            # Generate a root CA private key
            cur.execute("CREATE KEY vp_CA_key TYPE 'RSA' LENGTH 4096")
            # Generate a root CA certificate
            cur.execute("CREATE CA CERTIFICATE vp_CA_cert "
                    "SUBJECT '/C=US/ST=Massachusetts/L=Burlington/O=OpenText/OU=Vertica/CN=Vertica Root CA' "
                    "VALID FOR 3650 EXTENSIONS 'nsComment' = 'Self-signed root CA cert' KEY vp_CA_key")
            cur.execute("SELECT certificate_text FROM CERTIFICATES WHERE name='vp_CA_cert'")
            vp_CA_cert = cur.fetchone()[0]

            # Generate a server private key
            cur.execute("CREATE KEY vp_server_key TYPE 'RSA' LENGTH 4096")
            # Generate a server certificate
            host = self._conn_info['host']
            hostname_for_verify = ('IP:' if host.count('.') == 3 else 'DNS:') + host
            cur.execute("CREATE CERTIFICATE vp_server_cert "
                    "SUBJECT '/C=US/ST=MA/L=Cambridge/O=Foo/OU=Vertica/CN=Vertica server/emailAddress=abc@example.com' "
                    "SIGNED BY vp_CA_cert EXTENSIONS 'nsComment' = 'Vertica server cert', 'extendedKeyUsage' = 'serverAuth', "
                    f"'subjectAltName' = '{hostname_for_verify}' KEY vp_server_key")

            if mutual_mode:
                # Generate a client private key
                cur.execute("CREATE KEY vp_client_key TYPE 'RSA' LENGTH 4096")
                cur.execute("SELECT key FROM CRYPTOGRAPHIC_KEYS WHERE name='vp_client_key'")
                vp_client_key = cur.fetchone()[0]
                with NamedTemporaryFile(delete=False) as self.client_key:
                    self.client_key.write(vp_client_key.encode())
                # Generate a client certificate
                cur.execute("CREATE CERTIFICATE vp_client_cert "
                        "SUBJECT '/C=US/ST=MA/L=Boston/O=Bar/OU=Vertica/CN=Vertica client/emailAddress=def@example.com' "
                        "SIGNED BY vp_CA_cert EXTENSIONS 'nsComment' = 'Vertica client cert', 'extendedKeyUsage' = 'clientAuth' "
                        "KEY vp_client_key")
                cur.execute("SELECT certificate_text FROM CERTIFICATES WHERE name='vp_client_cert'")
                vp_client_cert = cur.fetchone()[0]
                with NamedTemporaryFile(delete=False) as self.client_cert:
                    self.client_cert.write(vp_client_cert.encode())

                # In order to use Mutual Mode, set a server and CA certificate.
                # This CA certificate is used to verify client certificates
                cur.execute('ALTER TLS CONFIGURATION server CERTIFICATE vp_server_cert ADD CA CERTIFICATES vp_CA_cert')
                # Enable TLS. Connection succeeds if Vertica verifies that the client certificate is from a trusted CA.
                # If the client does not present a client certificate, the connection uses plaintext.
                cur.execute("ALTER TLS CONFIGURATION server TLSMODE 'VERIFY_CA'")

            else:
                # In order to use Server Mode, set the server certificate for the server's TLS Configuration
                cur.execute('ALTER TLS CONFIGURATION server CERTIFICATE vp_server_cert')
                # Enable TLS. Server does not check client certificates.
                cur.execute("ALTER TLS CONFIGURATION server TLSMODE 'ENABLE'")

            # For debug
            # SELECT * FROM tls_configurations WHERE name='server';
            # SELECT * FROM CRYPTOGRAPHIC_KEYS;
            # SELECT * FROM CERTIFICATES;

            return vp_CA_cert


    def test_TLSMode_disable(self):
        self._conn_info['ssl'] = False
        with self._connect() as conn:
            cur = conn.cursor()
            res = self._query_and_fetchone('SELECT ssl_state FROM sessions WHERE session_id=(SELECT current_session())')
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
        ssl_context.check_hostname = True  # hostname in server cert's subjectAltName
        ssl_context.load_verify_locations(cadata=CA_cert) # CA certificate used to verify server certificate

        self._conn_info['ssl'] = ssl_context
        with self._connect() as conn:
            cur = conn.cursor()
            res = self._query_and_fetchone('SELECT ssl_state FROM sessions WHERE session_id=(SELECT current_session())')
            self.assertEqual(res[0], 'Server')

    def test_mutual_TLS(self):
        # Setting certificates with TLS configuration
        CA_cert = self._generate_and_set_certificates(mutual_mode=True)

        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.check_hostname = True  # hostname in server cert's subjectAltName
        ssl_context.load_verify_locations(cadata=CA_cert) # CA certificate used to verify server certificate
        ssl_context.load_cert_chain(certfile=self.client_cert.name, keyfile=self.client_key.name)

        self._conn_info['ssl'] = ssl_context
        with self._connect() as conn:
            cur = conn.cursor()
            res = self._query_and_fetchone('SELECT ssl_state FROM sessions WHERE session_id=(SELECT current_session())')
            self.assertEqual(res[0], 'Mutual')


exec(TlsTestCase.createPrepStmtClass())
