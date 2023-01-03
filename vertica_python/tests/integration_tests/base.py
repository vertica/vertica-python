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

import pytest

from ... import errors, connect
from ..common.base import VerticaPythonTestCase


@pytest.mark.integration_tests
class VerticaPythonIntegrationTestCase(VerticaPythonTestCase):
    """
    Base class for tests that connect to a Vertica database to run stuffs.

    This class is responsible for managing the environment variables and
    connection info used for all the tests, and provides support code
    to do common assertions and execute common queries.
    """

    @classmethod
    def setUpClass(cls):
        config_list = ['log_dir', 'log_level', 'host', 'port',
                       'user', 'password', 'database']
        cls.test_config = cls._load_test_config(config_list)

        # Test logger
        logfile = cls._setup_logger('integration_tests',
                      cls.test_config['log_dir'], cls.test_config['log_level'])

        # Connection info
        # Note: The server-side prepared statements is disabled here. Please
        #       see cls.createPrepStmtClass() below.
        cls._conn_info = {
            'host': cls.test_config['host'],
            'port': cls.test_config['port'],
            'database': cls.test_config['database'],
            'user': cls.test_config['user'],
            'password': cls.test_config['password'],
            'log_level': cls.test_config['log_level'],
            'log_path': logfile,
        }
        cls.db_node_num = cls._get_node_num()
        cls.logger.info("Number of database node(s) = {}".format(cls.db_node_num))

    @classmethod
    def tearDownClass(cls):
        pass

    @classmethod
    def _connect(cls):
        """Connects to vertica.

        :return: a connection to vertica.
        """
        return connect(**cls._conn_info)

    @classmethod
    def _get_node_num(cls):
        """Executes a query to get the number of nodes in the database

        :return: the number of database nodes
        """
        with cls._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM nodes WHERE node_state='UP'")
            return cur.fetchone()[0]

    @classmethod
    def createPrepStmtClass(cls):
        """Generates the code of a new subclass that has the same tests as this
        class but turns on the server-side prepared statements. To ensure test
        coverage, this method should be used if tests are not sensitive to
        paramstyles (or query protocols).
        Usage: "exec(xxxTestCase.createPrepStmtClass())"

        :return: a string acceptable by exec() to define the class
        """
        base_cls_name = cls.__name__
        cls_name = 'PrepStmt' + base_cls_name
        code = ('class ' + cls_name + '(' + base_cls_name + '):\n'
                '  @classmethod\n'
                '  def setUpClass(cls):\n'
                '    super(' + cls_name + ', cls).setUpClass()\n'
                "    cls._conn_info['use_prepared_statements'] = True")
        return code

    def _query_and_fetchall(self, query):
        """Creates a new connection, executes a query and fetches all the results.

        :param query: query to execute
        :return: all fetched results as returned by cursor.fetchall()
        """
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query)
            results = cur.fetchall()

        return results

    def _query_and_fetchone(self, query):
        """Creates a new connection, executes a query and fetches one result.

        :param query: query to execute
        :return: the first result fetched by cursor.fetchone()
        """
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query)
            result = cur.fetchone()

        return result

    # Common assertions
    def assertConnectionFail(self,
        err_type=errors.ConnectionError,
        err_msg='Failed to establish a connection to the primary server or any backup address.'):
        with pytest.raises(err_type, match=err_msg):
            with self._connect() as conn:
                pass

    def assertConnectionSuccess(self):
        try:
            with self._connect() as conn:
                pass
        except Exception as e:
            self.fail('Connection failed: {0}'.format(e))

    # Some tests require server-side setup
    # In that case, tests that depend on that setup should be skipped to prevent false failures
    # Tests that depend on the server-setup should call these methods to express requirements
    def require_DB_nodes_at_least(self, min_node_num):
        if not isinstance(min_node_num, int):
            err_msg = "Node number '{0}' must be an instance of 'int'".format(min_node_num)
            raise TypeError(err_msg)
        if min_node_num <= 0:
            err_msg = "Node number {0} must be a positive integer".format(min_node_num)
            raise ValueError(err_msg)

        if self.db_node_num < min_node_num:
            msg = ("The test requires a database that has at least {0} node(s), "
                   "but this database has only {1} available node(s).").format(
                   min_node_num, self.db_node_num)
            self.skipTest(msg)

    def require_protocol_at_least(self, min_protocol_version):
        with self._connect() as conn:
            effective_protocol = conn.parameters['protocol_version']
        if effective_protocol < min_protocol_version:
            msg = ("The test requires the effective protocol version to be at "
                   "least {}.{}, but the current version is {}.{}.").format(
                   min_protocol_version >> 16, min_protocol_version & 0x0000ffff,
                   effective_protocol >> 16, effective_protocol & 0x0000ffff)
            self.skipTest(msg)
