# Copyright (c) 2018-2023 Micro Focus or one of its affiliates.
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

from .base import VerticaPythonUnitTestCase
from ...vertica.connection import parse_dsn


class ParseDSNTestCase(VerticaPythonUnitTestCase):
    def test_basic(self):
        dsn = 'vertica://admin@192.168.10.1'
        expected = {'host': '192.168.10.1', 'user': 'admin'}
        parsed = parse_dsn(dsn)
        self.assertDictEqual(expected, parsed)

        dsn = 'vertica://mike@127.0.0.1/db1'
        expected = {'host': '127.0.0.1', 'user': 'mike', 'database': 'db1'}
        parsed = parse_dsn(dsn)
        self.assertDictEqual(expected, parsed)

        dsn = 'vertica://john:pwd@example.com:5433/db1'
        expected = {'database': 'db1', 'host': 'example.com', 'password': 'pwd',
                    'port': 5433, 'user': 'john'}
        parsed = parse_dsn(dsn)
        self.assertDictEqual(expected, parsed)

    def test_str_arguments(self):
        dsn = ('vertica://john:pwd@localhost:5433/db1?'
               'session_label=vpclient&unicode_error=strict&'
               'log_path=/home/admin/vClient.log&log_level=DEBUG&'
               'kerberos_service_name=krb_service&kerberos_host_name=krb_host')
        expected = {'database': 'db1', 'host': 'localhost', 'user': 'john',
                    'password': 'pwd', 'port': 5433, 'log_level': 'DEBUG',
                    'session_label': 'vpclient', 'unicode_error': 'strict',
                    'log_path': '/home/admin/vClient.log', 
                    'kerberos_service_name': 'krb_service',
                    'kerberos_host_name': 'krb_host'}
        parsed = parse_dsn(dsn)
        self.assertDictEqual(expected, parsed)

    def test_boolean_arguments(self):
        dsn = ('vertica://mike@127.0.0.1/db1?connection_load_balance=True&'
               'use_prepared_statements=0&ssl=false&disable_copy_local=on&'
               'autocommit=true&binary_transfer=1&request_complex_types=off')
        expected = {'database': 'db1', 'connection_load_balance': True,
                    'use_prepared_statements': False,  'ssl': False,
                    'disable_copy_local': True, 'autocommit': True,
                    'binary_transfer': True, 'request_complex_types': False,
                    'host': '127.0.0.1', 'user': 'mike'}
        parsed = parse_dsn(dsn)
        self.assertDictEqual(expected, parsed)

    def test_numeric_arguments(self):
        dsn = 'vertica://mike@127.0.0.1/db1?connection_timeout=1.5&log_level=10'
        expected = {'host': '127.0.0.1', 'user': 'mike', 'database': 'db1',
                    'connection_timeout': 1.5, 'log_level': 10}
        parsed = parse_dsn(dsn)
        self.assertDictEqual(expected, parsed)

    def test_ignored_arguments(self):
        # Invalid value
        dsn = ('vertica://mike@127.0.0.1/db1?ssl=ssl_context&'
               'connection_load_balance=unknown')
        expected = {'host': '127.0.0.1', 'user': 'mike', 'database': 'db1'}
        parsed = parse_dsn(dsn)
        self.assertDictEqual(expected, parsed)

        # Unsupported argument
        dsn = 'vertica://mike@127.0.0.1/db1?backup_server_node=123.456.789.123'
        expected = {'host': '127.0.0.1', 'user': 'mike', 'database': 'db1'}
        parsed = parse_dsn(dsn)
        self.assertDictEqual(expected, parsed)

    def test_overwrite_arguments(self):
        dsn = 'vertica://mike@127.0.0.1/db1?ssl=on&ssl=off&ssl=1&ssl=0'
        expected = {'host': '127.0.0.1', 'user': 'mike', 'database': 'db1',
                    'ssl': False}
        parsed = parse_dsn(dsn)
        self.assertDictEqual(expected, parsed)

    def test_arguments_blank_values(self):
        dsn = ('vertica://mike@127.0.0.1/db1?connection_timeout=1.5&log_path=&'
               'ssl=&connection_timeout=2&log_path=&connection_timeout=')
        expected = {'host': '127.0.0.1', 'user': 'mike', 'database': 'db1',
                    'connection_timeout': 2.0, 'log_path': ''}
        parsed = parse_dsn(dsn)
        self.assertDictEqual(expected, parsed)
