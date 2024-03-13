# Copyright (c) 2018-2024 Open Text.
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

from __future__ import print_function, division, absolute_import, annotations

import getpass
import socket
import uuid
from .base import VerticaPythonIntegrationTestCase


class ConnectionTestCase(VerticaPythonIntegrationTestCase):

    def tearDown(self):
        super(ConnectionTestCase, self).tearDown()
        if 'session_label' in self._conn_info:
            del self._conn_info['session_label']
        if 'autocommit' in self._conn_info:
            del self._conn_info['autocommit']
        if 'workload' in self._conn_info:
            del self._conn_info['workload']

    def test_client_os_user_name_metadata(self):
        try:
            value = getpass.getuser()
        except Exception as e:
            value = ''

        # Metadata client_os_user_name sent from client should be captured into system tables
        query = 'SELECT client_os_user_name FROM v_monitor.current_session'
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], value)

        query = 'SELECT client_os_user_name FROM v_monitor.sessions WHERE session_id=(SELECT current_session())'
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], value)

        query = 'SELECT client_os_user_name FROM v_monitor.user_sessions WHERE session_id=(SELECT current_session())'
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], value)

        query = 'SELECT client_os_user_name FROM v_internal.dc_session_starts WHERE session_id=(SELECT current_session())'
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], value)

    def test_client_os_hostname_metadata(self):
        self.require_protocol_at_least(3 << 16 | 14)
        try:
            value = socket.gethostname()
        except Exception as e:
            value = ''

        # Metadata client_os_hostname sent from client should be captured into system tables
        query = 'SELECT client_os_hostname FROM v_monitor.current_session'
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], value)

        query = 'SELECT client_os_hostname FROM v_monitor.sessions WHERE session_id=(SELECT current_session())'
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], value)

        query = 'SELECT client_os_hostname FROM v_monitor.user_sessions WHERE session_id=(SELECT current_session())'
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], value)

        query = 'SELECT client_os_hostname FROM v_internal.dc_session_starts WHERE session_id=(SELECT current_session())'
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], value)

    def test_session_label(self):
        label = str(uuid.uuid1())
        self._conn_info['session_label'] = label

        query = 'SELECT client_label FROM v_monitor.current_session'
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], label)

        query = 'SELECT client_label FROM v_monitor.sessions WHERE session_id=(SELECT current_session())'
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], label)

        query = 'SELECT client_label FROM v_monitor.user_sessions WHERE session_id=(SELECT current_session())'
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], label)

        query = 'SELECT client_label FROM v_internal.dc_session_starts WHERE session_id=(SELECT current_session())'
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], label)

    def test_autocommit_on(self):
        # Set with connection option
        self._conn_info['autocommit'] = True
        with self._connect() as conn:
            self.assertTrue(conn.autocommit)
            # Set with attribute setter
            conn.autocommit = False
            self.assertFalse(conn.autocommit)

    def test_autocommit_off(self):
        # Set with connection option
        self._conn_info['autocommit'] = False
        with self._connect() as conn:
            self.assertFalse(conn.autocommit)
            # Set with attribute setter
            conn.autocommit = True
            self.assertTrue(conn.autocommit)
    
    def test_workload_default(self):
        self.require_protocol_at_least(3 << 16 | 15)
        with self._connect() as conn:
            query = "SHOW WORKLOAD"
            res = self._query_and_fetchone(query)
            self.assertEqual(res[1], '')
    
    def test_workload_set_property(self):
        self.require_protocol_at_least(3 << 16 | 15)
        self._conn_info['workload'] = 'python_test_workload'
        with self._connect() as conn:
            # we use dc_client_server_messages to test that the client is working properly.
            # We do not regularly test on a multi subcluster database and the server will reject this
            # workload from the startup packet, returning a parameter status message with an empty string.
            query = ("SELECT contents FROM dc_client_server_messages"
                     " WHERE session_id = current_session()"
                     "  AND message_type = '^+'"
                     "  AND contents LIKE '%workload%'")
            res = self._query_and_fetchone(query)
            self.assertEqual(res[0], 'workload: python_test_workload')

exec(ConnectionTestCase.createPrepStmtClass())
