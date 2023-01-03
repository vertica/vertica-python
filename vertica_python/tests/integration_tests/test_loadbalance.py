# Copyright (c) 2018-2023 Micro Focus or one of its affiliates.
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

from __future__ import print_function, division, absolute_import

from .base import VerticaPythonIntegrationTestCase


class LoadBalanceTestCase(VerticaPythonIntegrationTestCase):
    def setUp(self):
        super(LoadBalanceTestCase, self).setUp()
        self._host = self._conn_info['host']
        self._port = self._conn_info['port']

    def tearDown(self):
        self._conn_info['host'] = self._host
        self._conn_info['port'] = self._port
        self._conn_info['connection_load_balance'] = False
        self._conn_info['backup_server_node'] = []

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT set_load_balance_policy('NONE')")
            cur.execute("DROP TABLE IF EXISTS test_loadbalance")
        super(LoadBalanceTestCase, self).tearDown()

    def get_node_num(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM nodes WHERE node_state='UP'")
            db_node_num = cur.fetchone()[0]
            return db_node_num

    def test_loadbalance_option_disabled(self):
        if 'connection_load_balance' in self._conn_info:
            del self._conn_info['connection_load_balance']
        self.assertConnectionSuccess()

        self._conn_info['connection_load_balance'] = False
        self.assertConnectionSuccess()

    def test_loadbalance_random(self):
        self.require_DB_nodes_at_least(3)
        self._conn_info['connection_load_balance'] = True
        rowsToInsert = 3 * self.db_node_num

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT set_load_balance_policy('RANDOM')")
            cur.execute("DROP TABLE IF EXISTS test_loadbalance")
            cur.execute("CREATE TABLE test_loadbalance (n varchar)")
            # record which node the client has connected to
            for i in range(rowsToInsert):
                with self._connect() as conn1:
                    cur1 = conn1.cursor()
                    cur1.execute("INSERT INTO test_loadbalance (SELECT node_name FROM sessions "
                                 "WHERE session_id = (SELECT current_session()))")
                    conn1.commit()

            cur.execute("SELECT count(DISTINCT n)>1 FROM test_loadbalance")
            res = cur.fetchone()
            self.assertTrue(res[0])

    def test_loadbalance_none(self):
        # Client turns on connection_load_balance but server is unsupported
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT set_load_balance_policy('NONE')")
        self._conn_info['connection_load_balance'] = True

        # Client will proceed with the existing connection with initiator
        self.assertConnectionSuccess()

        # Test for multi-node DB
        self.require_DB_nodes_at_least(3)
        rowsToInsert = 3 * self.db_node_num

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS test_loadbalance")
            cur.execute("CREATE TABLE test_loadbalance (n varchar)")
            # record which node the client has connected to
            for i in range(rowsToInsert):
                with self._connect() as conn1:
                    cur1 = conn1.cursor()
                    cur1.execute("INSERT INTO test_loadbalance (SELECT node_name FROM sessions "
                                 "WHERE session_id = (SELECT current_session()))")
                    conn1.commit()

            cur.execute("SELECT count(DISTINCT n)=1 FROM test_loadbalance")
            res = cur.fetchone()
            self.assertTrue(res[0])

    def test_loadbalance_roundrobin(self):
        self.require_DB_nodes_at_least(3)
        self._conn_info['connection_load_balance'] = True
        rowsToInsert = 3 * self.db_node_num

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT set_load_balance_policy('ROUNDROBIN')")
            cur.execute("DROP TABLE IF EXISTS test_loadbalance")
            cur.execute("CREATE TABLE test_loadbalance (n varchar)")
            # record which node the client has connected to
            for i in range(rowsToInsert):
                with self._connect() as conn1:
                    cur1 = conn1.cursor()
                    cur1.execute("INSERT INTO test_loadbalance (SELECT node_name FROM sessions "
                                 "WHERE session_id = (SELECT current_session()))")
                    conn1.commit()

            cur.execute("SELECT count(n)=3 FROM test_loadbalance GROUP BY n")
            res = cur.fetchall()
            # verify that all db_node_num nodes are represented equally
            self.assertEqual(len(res), self.db_node_num)
            for i in res:
                self.assertEqual(i, [True])

    def test_failover_empty_backup(self):
        # Connect to primary server
        if 'backup_server_node' in self._conn_info:
            del self._conn_info['backup_server_node']
        self.assertConnectionSuccess()
        self._conn_info['backup_server_node'] = []
        self.assertConnectionSuccess()

        # Set primary server to invalid host and port
        self._conn_info['host'] = 'invalidhost'
        self._conn_info['port'] = 9999

        # Fail to connect to primary server
        self.assertConnectionFail()

    def test_failover_one_backup(self):
        # Set primary server to invalid host and port
        self._conn_info['host'] = 'invalidhost'
        self._conn_info['port'] = 9999

        # One valid address in backup_server_node: port is an integer
        self._conn_info['backup_server_node'] = [(self._host, self._port)]
        self.assertConnectionSuccess()

        # One valid address in backup_server_node: port is a string
        self._conn_info['backup_server_node'] = [(self._host, str(self._port))]
        self.assertConnectionSuccess()

        # One invalid address in backup_server_node: DNS failed, Name or service not known
        self._conn_info['backup_server_node'] = [('invalidhost2', 8888)]
        self.assertConnectionFail()

        # One invalid address in backup_server_node: DNS failed, Name or service not known
        self._conn_info['backup_server_node'] = [('123.456.789.123', 8888)]
        self.assertConnectionFail()

        # One invalid address in backup_server_node: DNS failed, Address family for hostname not supported
        self._conn_info['backup_server_node'] = [('fd76:6572:7469:6361:0:242:ac11:4', 8888)]
        self.assertConnectionFail()

        # One invalid address in backup_server_node: Wrong port, connection refused
        self._conn_info['backup_server_node'] = [(self._host, 8888)]
        self.assertConnectionFail()

    def test_failover_multi_backup(self):
        # Set primary server to invalid host and port
        self._conn_info['host'] = 'invalidhost'
        self._conn_info['port'] = 9999

        # One valid and two invalid addresses in backup_server_node
        self._conn_info['backup_server_node'] = [(self._host, self._port), 'invalidhost2', 'foo']
        self.assertConnectionSuccess()
        self._conn_info['backup_server_node'] = ['foo', (self._host, self._port), ('123.456.789.1', 888)]
        self.assertConnectionSuccess()
        self._conn_info['backup_server_node'] = ['foo', ('invalidhost2', 8888), (self._host, self._port)]
        self.assertConnectionSuccess()

        # Three invalid addresses in backup_server_node
        self._conn_info['backup_server_node'] = ['foo', (self._host, 9999), ('123.456.789.1', 888)]
        self.assertConnectionFail()

    def test_failover_backup_format(self):
        # Set primary server to invalid host and port
        self._conn_info['host'] = 'invalidhost'
        self._conn_info['port'] = 9999

        err_msg = 'Connection option "backup_server_node" must be a list'
        self._conn_info['backup_server_node'] = (self._host, self._port)
        self.assertConnectionFail(TypeError, err_msg)

        err_msg = ('Each item of connection option "backup_server_node"'
                   r' must be a host string or a \(host, port\) tuple')
        self._conn_info['backup_server_node'] = [9999]
        self.assertConnectionFail(TypeError, err_msg)
        self._conn_info['backup_server_node'] = [(self._host, self._port, 'foo', 9999)]
        self.assertConnectionFail(TypeError, err_msg)

        err_msg = 'Host must be a string: invalid value: .*'
        self._conn_info['backup_server_node'] = [(9999, self._port)]
        self.assertConnectionFail(TypeError, err_msg)
        self._conn_info['backup_server_node'] = [(9999, 'port_num')]
        self.assertConnectionFail(TypeError, err_msg)

        err_msg = 'Port must be an integer or a string: invalid value: .*'
        self._conn_info['backup_server_node'] = [(self._host, 5433.0022)]
        self.assertConnectionFail(TypeError, err_msg)

        err_msg = r'Port .* is not a valid string: invalid literal for int\(\) with base 10: .*'
        self._conn_info['backup_server_node'] = [(self._host, 'port_num')]
        self.assertConnectionFail(ValueError, err_msg)
        self._conn_info['backup_server_node'] = [(self._host, '5433.0022')]
        self.assertConnectionFail(ValueError, err_msg)

        err_msg = 'Invalid port number: .*'
        self._conn_info['backup_server_node'] = [(self._host, -1000)]
        self.assertConnectionFail(ValueError, err_msg)
        self._conn_info['backup_server_node'] = [(self._host, 66000)]
        self.assertConnectionFail(ValueError, err_msg)
        self._conn_info['backup_server_node'] = [(self._host, '-1000')]
        self.assertConnectionFail(ValueError, err_msg)
        self._conn_info['backup_server_node'] = [(self._host, '66000')]
        self.assertConnectionFail(ValueError, err_msg)

    def test_failover_with_loadbalance_roundrobin(self):
        self.require_DB_nodes_at_least(3)

        # Set primary server to invalid host and port
        self._conn_info['host'] = 'invalidhost'
        self._conn_info['port'] = 9999
        self.assertConnectionFail()

        self._conn_info['backup_server_node'] = [('invalidhost2', 8888), (self._host, self._port)]
        self.assertConnectionSuccess()

        self._conn_info['connection_load_balance'] = True
        rowsToInsert = 3 * self.db_node_num

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT set_load_balance_policy('ROUNDROBIN')")
            cur.execute("DROP TABLE IF EXISTS test_loadbalance")
            cur.execute("CREATE TABLE test_loadbalance (n varchar)")
            # record which node the client has connected to
            for i in range(rowsToInsert):
                with self._connect() as conn1:
                    cur1 = conn1.cursor()
                    cur1.execute("INSERT INTO test_loadbalance ("
                                 "SELECT node_name FROM sessions "
                                 "WHERE session_id = (SELECT current_session()))")
                    conn1.commit()

            cur.execute("SELECT count(n)=3 FROM test_loadbalance GROUP BY n")
            res = cur.fetchall()
            # verify that all db_node_num nodes are represented equally
            self.assertEqual(len(res), self.db_node_num)
            for i in res:
                self.assertEqual(i, [True])


exec(LoadBalanceTestCase.createPrepStmtClass())
