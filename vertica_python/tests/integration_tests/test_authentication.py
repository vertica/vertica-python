# Copyright (c) 2018-2023 Open Text.
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

from .base import VerticaPythonIntegrationTestCase
from ...errors import OAuthTokenRefreshError


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
        if 'oauth_refresh_token' in self._conn_info:
            del self._conn_info['oauth_refresh_token']
        if 'oauth_config' in self._conn_info:
            del self._conn_info['oauth_config']
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

    def _test_oauth_refresh(self, access_token):
        self.require_protocol_at_least(3 << 16 | 11)
        if not self._oauth_info['refresh_token']:
            self.skipTest('OAuth refresh token not set')
        if not (self._oauth_info['client_secret'] and self._oauth_info['client_id'] and self._oauth_info['token_url']):
            self.skipTest('One or more OAuth config (client_id, client_secret, token_url) not set')
        if not self._oauth_info['user'] and not self._conn_info['database']:
            self.skipTest('Both database and oauth_user are not set')

        if access_token is not None:
            self._conn_info['oauth_access_token'] = access_token
        self._conn_info['user'] = self._oauth_info['user']
        self._conn_info['oauth_refresh_token'] = self._oauth_info['refresh_token']
        self._conn_info['oauth_config'] = {
                'client_secret': self._oauth_info['client_secret'],
                'client_id': self._oauth_info['client_id'],
                'token_url': self._oauth_info['token_url'],
        }
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT authentication_method FROM sessions WHERE session_id=(SELECT current_session())")
            res = cur.fetchone()
            self.assertEqual(res[0], 'OAuth')

    def test_oauth_token_refresh_with_access_token_not_set(self):
        self._test_oauth_refresh(access_token=None)

    def test_oauth_token_refresh_with_invalid_access_token(self):
        self._test_oauth_refresh(access_token='invalid_value')

    def test_oauth_token_refresh_with_empty_access_token(self):
        self._test_oauth_refresh(access_token='')

    def test_oauth_token_refresh_with_discovery_url(self):
        self.require_protocol_at_least(3 << 16 | 11)
        if not self._oauth_info['refresh_token']:
            self.skipTest('OAuth refresh token not set')
        if not (self._oauth_info['client_secret'] and self._oauth_info['client_id'] and self._oauth_info['discovery_url']):
            self.skipTest('One or more OAuth config (client_id, client_secret, discovery_url) not set')
        if not self._oauth_info['user'] and not self._conn_info['database']:
            self.skipTest('Both database and oauth_user are not set')

        self._conn_info['user'] = self._oauth_info['user']
        self._conn_info['oauth_refresh_token'] = self._oauth_info['refresh_token']
        msg = 'Token URL or Discovery URL must be set.'
        self.assertConnectionFail(err_type=OAuthTokenRefreshError, err_msg=msg)

        self._conn_info['oauth_config'] = {
                'client_secret': self._oauth_info['client_secret'],
                'client_id': self._oauth_info['client_id'],
                'discovery_url': self._oauth_info['discovery_url'],
        }
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT authentication_method FROM sessions WHERE session_id=(SELECT current_session())")
            res = cur.fetchone()
            self.assertEqual(res[0], 'OAuth')

        # Token URL takes precedence over Discovery URL
        self._conn_info['oauth_config']['token_url'] = 'invalid_value'
        self.assertConnectionFail(err_type=OAuthTokenRefreshError, err_msg='Failed getting OAuth access token from a refresh token.')


exec(AuthenticationTestCase.createPrepStmtClass())
