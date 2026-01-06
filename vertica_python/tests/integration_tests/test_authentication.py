# Copyright (c) 2018-2024 Open Text.
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

from __future__ import annotations

from .base import VerticaPythonIntegrationTestCase


class AuthenticationTestCase(VerticaPythonIntegrationTestCase):
    def setUp(self):
        super(AuthenticationTestCase, self).setUp()
        self._user = self._conn_info['user']
        self._password = self._conn_info['password']

    def tearDown(self):
        self._conn_info['user'] = self._user
        self._conn_info['password'] = self._password
        if 'oauth_access_token' in self._conn_info:
            del self._conn_info['oauth_access_token']
        super(AuthenticationTestCase, self).tearDown()

    def test_SHA512(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("DROP USER IF EXISTS sha512_user")
            cur.execute("DROP AUTHENTICATION IF EXISTS testIPv4hostHash CASCADE")
            cur.execute("DROP AUTHENTICATION IF EXISTS testIPv6hostHash CASCADE")
            cur.execute("DROP AUTHENTICATION IF EXISTS testlocalHash CASCADE")
            try:
                cur.execute("CREATE USER sha512_user IDENTIFIED BY 'password'")
                cur.execute("ALTER USER sha512_user SECURITY_ALGORITHM 'SHA512'")
                cur.execute("CREATE AUTHENTICATION testIPv4hostHash METHOD 'hash' HOST '0.0.0.0/0'")
                cur.execute("GRANT AUTHENTICATION testIPv4hostHash TO sha512_user")
                cur.execute("CREATE AUTHENTICATION testIPv6hostHash METHOD 'hash' HOST '::/0'")
                cur.execute("GRANT AUTHENTICATION testIPv6hostHash TO sha512_user")
                cur.execute("CREATE AUTHENTICATION testlocalHash METHOD 'hash' LOCAL")
                cur.execute("GRANT AUTHENTICATION testlocalHash TO sha512_user")

                self._conn_info['user'] = 'sha512_user'
                self._conn_info['password'] = 'password'

                expire_msg = 'The password for user sha512_user has expired'
                self.assertConnectionFail(err_msg=expire_msg)

                # Test SHA512 connection
                cur.execute("ALTER USER sha512_user IDENTIFIED BY 'password'")
                self.assertConnectionSuccess()

                # Switch to MD5 for hashing and storing the user password
                cur.execute("ALTER USER sha512_user SECURITY_ALGORITHM 'MD5'")
                self.assertConnectionFail(err_msg=expire_msg)

                # Test MD5 connection
                cur.execute("ALTER USER sha512_user IDENTIFIED BY 'password'")
                self.assertConnectionSuccess()
            finally:
                # Must clean up authentication methods within this session, no
                # matter whether an exception has occurred or not, otherwise
                # those authentication methods may affect existing users
                cur.execute("DROP USER IF EXISTS sha512_user")
                cur.execute("DROP AUTHENTICATION IF EXISTS testIPv4hostHash CASCADE")
                cur.execute("DROP AUTHENTICATION IF EXISTS testIPv6hostHash CASCADE")
                cur.execute("DROP AUTHENTICATION IF EXISTS testlocalHash CASCADE")

    def test_password_expire(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("DROP USER IF EXISTS pw_expire_user")
            cur.execute("DROP AUTHENTICATION IF EXISTS testIPv4hostHash CASCADE")
            cur.execute("DROP AUTHENTICATION IF EXISTS testIPv6hostHash CASCADE")
            cur.execute("DROP AUTHENTICATION IF EXISTS testlocalHash CASCADE")
            try:
                cur.execute("CREATE USER pw_expire_user IDENTIFIED BY 'password'")
                cur.execute("CREATE AUTHENTICATION testIPv4hostHash METHOD 'hash' HOST '0.0.0.0/0'")
                cur.execute("GRANT AUTHENTICATION testIPv4hostHash TO pw_expire_user")
                cur.execute("CREATE AUTHENTICATION testIPv6hostHash METHOD 'hash' HOST '::/0'")
                cur.execute("GRANT AUTHENTICATION testIPv6hostHash TO pw_expire_user")
                cur.execute("CREATE AUTHENTICATION testlocalHash METHOD 'hash' LOCAL")
                cur.execute("GRANT AUTHENTICATION testlocalHash TO pw_expire_user")

                # Test connection
                self._conn_info['user'] = 'pw_expire_user'
                self._conn_info['password'] = 'password'
                self.assertConnectionSuccess()

                # Expire the user's password immediately
                cur.execute("ALTER USER pw_expire_user PASSWORD EXPIRE")

                # Expect an error message
                self.assertConnectionFail(
                    err_msg='The password for user pw_expire_user has expired')
            finally:
                # Must clean up authentication methods within this session, no
                # matter whether an exception has occurred or not, otherwise
                # those authentication methods may affect existing users
                cur.execute("DROP USER IF EXISTS pw_expire_user")
                cur.execute("DROP AUTHENTICATION IF EXISTS testIPv4hostHash CASCADE")
                cur.execute("DROP AUTHENTICATION IF EXISTS testIPv6hostHash CASCADE")
                cur.execute("DROP AUTHENTICATION IF EXISTS testlocalHash CASCADE")

    def test_oauth_access_token(self):
        self.require_protocol_at_least(3 << 16 | 11)
        if not self._oauth_info['access_token']:
            self.skipTest('OAuth access token not set')
        if not self._oauth_info['user'] and not self._conn_info['database']:
            self.skipTest('Both database and oauth_user are not set')

        self._conn_info['user'] = self._oauth_info['user']
        self._conn_info['oauth_access_token'] = self._oauth_info['access_token']
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT authentication_method FROM sessions WHERE session_id=(SELECT current_session())")
            res = cur.fetchone()
            self.assertEqual(res[0], 'OAuth')
    # -------------------------------
    # TOTP Authentication Test for Vertica-Python Driver
    # -------------------------------
    import os
    import pyotp
    from io import StringIO
    import sys


    # Positive TOTP Test (Like SHA512 format)
    def totp_positive_scenario(self):
        with self._connect() as conn:
            cur = conn.cursor()

            cur.execute("DROP USER IF EXISTS totp_user")
            cur.execute("DROP AUTHENTICATION IF EXISTS totp_auth CASCADE")

            try:
                # Create user with MFA
                cur.execute("CREATE USER totp_user IDENTIFIED BY 'password' ENFORCEMFA")

                # Grant authentication
                # Note: METHOD is 'trusted' or 'password' depending on how MFA is enforced in Vertica
                cur.execute("CREATE AUTHENTICATION totp_auth METHOD 'password' HOST '0.0.0.0/0'")
                cur.execute("GRANT AUTHENTICATION totp_auth TO totp_user")

                # Generate TOTP
                TOTP_SECRET = "O5D7DQICJTM34AZROWHSAO4O53ELRJN3"
                totp_code = pyotp.TOTP(TOTP_SECRET).now()

                # Set connection info
                self._conn_info['user'] = 'totp_user'
                self._conn_info['password'] = 'password'
                self._conn_info['totp'] = totp_code

                # Try connection
                with self._connect() as totp_conn:
                    c = totp_conn.cursor()
                    c.execute("SELECT 1")
                    res = c.fetchone()
                    self.assertEqual(res[0], 1)

            finally:
                cur.execute("DROP USER IF EXISTS totp_user")
                cur.execute("DROP AUTHENTICATION IF EXISTS totp_auth CASCADE")

    # Negative Test: Missing TOTP
    def totp_missing_code_scenario(self):
        with self._connect() as conn:
            cur = conn.cursor()

            cur.execute("DROP USER IF EXISTS totp_user")
            cur.execute("DROP AUTHENTICATION IF EXISTS totp_auth CASCADE")

            try:
                cur.execute("CREATE USER totp_user IDENTIFIED BY 'password' ENFORCEMFA")
                cur.execute("CREATE AUTHENTICATION totp_auth METHOD 'password' HOST '0.0.0.0/0'")
                cur.execute("GRANT AUTHENTICATION totp_auth TO totp_user")

                self._conn_info['user'] = 'totp_user'
                self._conn_info['password'] = 'password'
                self._conn_info.pop('totp', None)  # No TOTP

                err_msg = "TOTP was requested but not provided"
                self.assertConnectionFail(err_msg=err_msg)

            finally:
                cur.execute("DROP USER IF EXISTS totp_user")
                cur.execute("DROP AUTHENTICATION IF EXISTS totp_auth CASCADE")

    # Negative Test: Invalid TOTP Format
    def totp_invalid_format_scenario(self):
        with self._connect() as conn:
            cur = conn.cursor()

            cur.execute("DROP USER IF EXISTS totp_user")
            cur.execute("DROP AUTHENTICATION IF EXISTS totp_auth CASCADE")

            try:
                cur.execute("CREATE USER totp_user IDENTIFIED BY 'password' ENFORCEMFA")
                cur.execute("CREATE AUTHENTICATION totp_auth METHOD 'password' HOST '0.0.0.0/0'")
                cur.execute("GRANT AUTHENTICATION totp_auth TO totp_user")

                self._conn_info['user'] = 'totp_user'
                self._conn_info['password'] = 'password'
                self._conn_info['totp'] = "123"   # Invalid

                err_msg = "Invalid TOTP format"
                self.assertConnectionFail(err_msg=err_msg)

            finally:
                cur.execute("DROP USER IF EXISTS totp_user")
                cur.execute("DROP AUTHENTICATION IF EXISTS totp_auth CASCADE")

    def test_totp_invalid_alphanumeric_code(self):
        # Verify alphanumeric TOTP inputs return the explicit validation error
        with self._connect() as conn:
            cur = conn.cursor()

            cur.execute("DROP USER IF EXISTS totp_user")
            cur.execute("DROP AUTHENTICATION IF EXISTS totp_auth CASCADE")

            try:
                cur.execute("CREATE USER totp_user IDENTIFIED BY 'password' ENFORCEMFA")
                cur.execute("CREATE AUTHENTICATION totp_auth METHOD 'password' HOST '0.0.0.0/0'")
                cur.execute("GRANT AUTHENTICATION totp_auth TO totp_user")

                self._conn_info['user'] = 'totp_user'
                self._conn_info['password'] = 'password'
                # Alphanumeric TOTP provided via driver parameter
                self._conn_info['totp'] = "ot123"

                err_msg = "Invalid TOTP: Please enter a valid 6-digit numeric code"
                self.assertConnectionFail(err_msg=err_msg)

            finally:
                cur.execute("DROP USER IF EXISTS totp_user")
                cur.execute("DROP AUTHENTICATION IF EXISTS totp_auth CASCADE")

    # Negative Test: Wrong TOTP (Valid format, wrong value)
    def totp_wrong_code_scenario(self):
        with self._connect() as conn:
            cur = conn.cursor()

            cur.execute("DROP USER IF EXISTS totp_user")
            cur.execute("DROP AUTHENTICATION IF EXISTS totp_auth CASCADE")

            try:
                cur.execute("CREATE USER totp_user IDENTIFIED BY 'password' ENFORCEMFA")
                cur.execute("CREATE AUTHENTICATION totp_auth METHOD 'password' HOST '0.0.0.0/0'")
                cur.execute("GRANT AUTHENTICATION totp_auth TO totp_user")

                self._conn_info['user'] = 'totp_user'
                self._conn_info['password'] = 'password'
                self._conn_info['totp'] = "999999"   # Wrong OTP

                err_msg = "Invalid TOTP"
                self.assertConnectionFail(err_msg=err_msg)

            finally:
                cur.execute("DROP USER IF EXISTS totp_user")
                cur.execute("DROP AUTHENTICATION IF EXISTS totp_auth CASCADE")

