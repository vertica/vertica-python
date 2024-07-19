# Copyright (c) 2023-2024 Open Text.
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
    SSL_STATE_SQL = 'SELECT ssl_state FROM sessions WHERE session_id=current_session()'

    def tearDown(self):
        # Use a non-TLS connection here so cleanup can happen
        # even if mutual mode is configured
        self._conn_info['tlsmode'] = 'disable'
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("ALTER TLS CONFIGURATION server CERTIFICATE NULL TLSMODE 'DISABLE'")
            if hasattr(self, 'client_cert'):
                os.remove(self.client_cert.name)
                cur.execute("ALTER TLS CONFIGURATION server REMOVE CA CERTIFICATES vp_CA_cert")
            if hasattr(self, 'client_key'):
                os.remove(self.client_key.name)
                cur.execute("DROP KEY IF EXISTS vp_client_key CASCADE")
            if hasattr(self, 'CA_cert'):
                os.remove(self.CA_cert.name)
            cur.execute("DROP KEY IF EXISTS vp_server_key CASCADE")
            cur.execute("DROP KEY IF EXISTS vp_CA_key CASCADE")

        for key in ('tlsmode', 'ssl', 'tls_cafile', 'tls_certfile', 'tls_keyfile'):
            if key in self._conn_info:
                del self._conn_info[key]
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
            with NamedTemporaryFile(delete=False) as self.CA_cert:
                    self.CA_cert.write(vp_CA_cert.encode())

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
                # If the client does not present a client certificate, the connection is rejected.
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

    ######################################################
    #### Test 'ssl' and 'tlsmode' options are not set ####
    ######################################################

    def test_option_default_server_disable(self):
        # TLS is disabled on the server
        with self._connect() as conn:
            cur = conn.cursor()
            res = self._query_and_fetchone(self.SSL_STATE_SQL)
            self.assertEqual(res[0], 'None')

    def test_option_default_server_enable(self):
        # Setting certificates with TLS configuration
        self._generate_and_set_certificates()

        # TLS is enabled on the server
        with self._connect() as conn:
            cur = conn.cursor()
            res = self._query_and_fetchone(self.SSL_STATE_SQL)
            self.assertEqual(res[0], 'Server')

    #######################################################
    #### Test 'ssl' and 'tlsmode' options are both set ####
    #######################################################

    def test_tlsmode_over_ssl(self):
        self._conn_info['tlsmode'] = 'disable'
        self._conn_info['ssl'] = True
        with self._connect() as conn:
            cur = conn.cursor()
            res = self._query_and_fetchone(self.SSL_STATE_SQL)
            self.assertEqual(res[0], 'None')

    ###############################################
    #### Test 'ssl' option with boolean values ####
    ###############################################

    def test_ssl_false(self):
        self._conn_info['ssl'] = False
        with self._connect() as conn:
            cur = conn.cursor()
            res = self._query_and_fetchone(self.SSL_STATE_SQL)
            self.assertEqual(res[0], 'None')

    def test_ssl_true_server_disable(self):
        # Requires that the server use TLS. If the TLS connection attempt fails, the client rejects the connection.
        self._conn_info['ssl'] = True
        self.assertConnectionFail(err_type=errors.SSLNotSupported,
                err_msg='SSL requested but disabled on the server')

    def test_ssl_true_server_enable(self):
        # Setting certificates with TLS configuration
        self._generate_and_set_certificates()

        self._conn_info['ssl'] = True
        with self._connect() as conn:
            cur = conn.cursor()
            res = self._query_and_fetchone(self.SSL_STATE_SQL)
            self.assertEqual(res[0], 'Server')

    ###############################
    #### Test 'tlsmode' option ####
    ###############################

    def test_TLSMode_disable(self):
        self._conn_info['tlsmode'] = 'disable'
        with self._connect() as conn:
            cur = conn.cursor()
            res = self._query_and_fetchone(self.SSL_STATE_SQL)
            self.assertEqual(res[0], 'None')

    def test_TLSMode_prefer_server_disable(self):
        # TLS is disabled on the server
        self._conn_info['tlsmode'] = 'prefer'
        with self._connect() as conn:
            cur = conn.cursor()
            res = self._query_and_fetchone(self.SSL_STATE_SQL)
            self.assertEqual(res[0], 'None')

    def test_TLSMode_prefer_server_enable(self):
        # Setting certificates with TLS configuration
        self._generate_and_set_certificates()

        self._conn_info['tlsmode'] = 'prefer'
        with self._connect() as conn:
            cur = conn.cursor()
            res = self._query_and_fetchone(self.SSL_STATE_SQL)
            self.assertEqual(res[0], 'Server')

    def test_TLSMode_require_server_disable(self):
        # Requires that the server use TLS. If the TLS connection attempt fails, the client rejects the connection.
        self._conn_info['tlsmode'] = 'require'
        self.assertConnectionFail(err_type=errors.SSLNotSupported,
                err_msg='SSL requested but disabled on the server')

    def test_TLSMode_require_server_enable(self):
        # Setting certificates with TLS configuration
        self._generate_and_set_certificates()

        self._conn_info['tlsmode'] = 'require'
        with self._connect() as conn:
            cur = conn.cursor()
            res = self._query_and_fetchone(self.SSL_STATE_SQL)
            self.assertEqual(res[0], 'Server')

    def test_TLSMode_verify_ca(self):
        # Setting certificates with TLS configuration
        CA_cert = self._generate_and_set_certificates()

        self._conn_info['tlsmode'] = 'verify-ca'
        self._conn_info['tls_cafile'] = self.CA_cert.name
        with self._connect() as conn:
            cur = conn.cursor()
            res = self._query_and_fetchone(self.SSL_STATE_SQL)
            self.assertEqual(res[0], 'Server')

    def test_TLSMode_verify_full(self):
        # Setting certificates with TLS configuration
        CA_cert = self._generate_and_set_certificates()

        self._conn_info['tlsmode'] = 'verify-full'
        self._conn_info['tls_cafile'] = self.CA_cert.name
        with self._connect() as conn:
            cur = conn.cursor()
            res = self._query_and_fetchone(self.SSL_STATE_SQL)
            self.assertEqual(res[0], 'Server')

    def test_TLSMode_mutual_TLS(self):
        # Setting certificates with TLS configuration
        CA_cert = self._generate_and_set_certificates(mutual_mode=True)

        self._conn_info['tlsmode'] = 'verify-full'
        self._conn_info['tls_cafile'] = self.CA_cert.name  # CA certificate used to verify server certificate
        self._conn_info['tls_certfile'] = self.client_cert.name  # client certificate
        self._conn_info['tls_keyfile'] = self.client_key.name  # private key used for the client certificate
        with self._connect() as conn:
            cur = conn.cursor()
            res = self._query_and_fetchone(self.SSL_STATE_SQL)
            self.assertEqual(res[0], 'Mutual')

    ######################################################
    #### Test 'ssl' option with ssl.SSLContext object ####
    ######################################################

    def test_sslcontext_require(self):
        # Setting certificates with TLS configuration
        self._generate_and_set_certificates()

        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        self._conn_info['ssl'] = ssl_context
        with self._connect() as conn:
            cur = conn.cursor()
            res = self._query_and_fetchone(self.SSL_STATE_SQL)
            self.assertEqual(res[0], 'Server')

    def test_sslcontext_verify_ca(self):
        # Setting certificates with TLS configuration
        CA_cert = self._generate_and_set_certificates()

        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.check_hostname = False
        ssl_context.load_verify_locations(cadata=CA_cert) # CA certificate used to verify server certificate
        self._conn_info['ssl'] = ssl_context

        with self._connect() as conn:
            cur = conn.cursor()
            res = self._query_and_fetchone(self.SSL_STATE_SQL)
            self.assertEqual(res[0], 'Server')

    def test_sslcontext_verify_full(self):
        # Setting certificates with TLS configuration
        CA_cert = self._generate_and_set_certificates()

        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.check_hostname = True  # hostname in server cert's subjectAltName
        ssl_context.load_verify_locations(cadata=CA_cert) # CA certificate used to verify server certificate

        self._conn_info['ssl'] = ssl_context
        with self._connect() as conn:
            cur = conn.cursor()
            res = self._query_and_fetchone(self.SSL_STATE_SQL)
            self.assertEqual(res[0], 'Server')

    def test_sslcontext_mutual_TLS(self):
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
            res = self._query_and_fetchone(self.SSL_STATE_SQL)
            self.assertEqual(res[0], 'Mutual')


exec(TlsTestCase.createPrepStmtClass())
