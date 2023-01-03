# Copyright (c) 2022-2023 Micro Focus or one of its affiliates.
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

from .base import VerticaPythonIntegrationTestCase


"""Check the consistency of query results btw text transfer and binary transfer"""
class DataTransferFormatTestCase(VerticaPythonIntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        super(DataTransferFormatTestCase, cls).setUpClass()
        cls._conn_info['binary_transfer'] = False
        cls.text_conn = cls._connect()
        cls._conn_info['binary_transfer'] = True
        cls.binary_conn = cls._connect()
        cls.text_cursor = cls.text_conn.cursor()
        cls.binary_cursor = cls.binary_conn.cursor()

    @classmethod
    def tearDownClass(cls):
        cls.text_conn.close()
        cls.binary_conn.close()

    def _test_equal_value(self, sql_type, data_list, assert_almost_equal=False):
        for data in data_list:
            query = u"SELECT {}{}".format(data, "::" + sql_type if sql_type else '')
            self.text_cursor.execute(query)
            self.binary_cursor.execute(query)
            text_val = self.text_cursor.fetchone()[0]
            binary_val = self.binary_cursor.fetchone()[0]
            if assert_almost_equal:
                self.assertAlmostEqual(text_val, binary_val)
            else:
                self.assertEqual(text_val, binary_val)

    def test_boolean_type(self):
        self._test_equal_value("BOOLEAN", ['true', 'false'])

    def test_integer_type(self):
        self._test_equal_value("INTEGER", ["-314", "0", "365", "111111111111"])

    def test_float_type(self):
        self._test_equal_value("FLOAT", [
            "'Infinity'", "'-Infinity'",
            "'1.23456e+18'", "'1.23456'", "'1.23456e-18'"])
        # binary transfer offers slightly greater precision than text transfer
        # binary: 1.489968353486419
        # text:   1.48996835348642
        self._test_equal_value(None, ["ATAN(12.345)"], True)

    def test_numeric_type(self):
        self._test_equal_value("NUMERIC", ["0", "-1.1", "1234567890123456789.0123456789"])
        self._test_equal_value("DECIMAL", ["123456789.98765"])

    def test_char_type(self):
        self._test_equal_value("CHAR(8)", [u"'\u16b1'"])
        self._test_equal_value("VARCHAR", [u"'foo\u16b1'"])
        self._test_equal_value("LONG VARCHAR", [u"'foo \u16b1 bar'"])

    def test_datetime_type(self):
        self._test_equal_value("DATE", ["'0340-01-20'", "'2001-12-01'", "'9999-12-31'"])
        self._test_equal_value("TIME(3)", ["'00:00:00.00'", "'22:36:33.123956'", "'23:59:59.999'"])
        self._test_equal_value("TIMETZ(3)", ["'23:59:59.999-00:30'", "'22:36:33.123456+0630'", "'800-02-03 22:36:33.123456 America/Cayman'"])
        self._test_equal_value("TIMESTAMP", ["'276-12-1 11:22:33'", "'2001-12-01 00:30:45.087'"])
        self._test_equal_value("TIMESTAMPTZ(4)", ["'1582-09-24 00:30:45.087-08'", "'0001-1-1 11:22:33'", "'2020-12-31 10:43:09.05'"])

    def test_interval_type(self):
        self._test_equal_value("INTERVAL DAY TO SECOND", ["'1 02:03:04.0005'", "'1 02:03:04'", "'02:03:04.0005'", "'02:03'"])
        self._test_equal_value("INTERVAL DAY TO MINUTE", ["'1 02:03'", "'02:03'"])
        self._test_equal_value("INTERVAL DAY TO HOUR", ["'1 22'"])
        self._test_equal_value("INTERVAL DAY", ["'132'"])
        self._test_equal_value("INTERVAL HOUR TO SECOND", ["'02:03:04'"])
        self._test_equal_value("INTERVAL HOUR TO MINUTE", ["'02:03'"])
        self._test_equal_value("INTERVAL HOUR", ["'02'"])
        self._test_equal_value("INTERVAL MINUTE TO SECOND", ["'00:04.0005'", "'03:04'"])
        self._test_equal_value("INTERVAL MINUTE", ["'03'"])
        self._test_equal_value("INTERVAL SECOND", ["'216901.24'", "'216901'"])
        self._test_equal_value("INTERVAL YEAR", ["'1y 10m'"])
        self._test_equal_value("INTERVAL YEAR TO MONTH", ["'1y 10m'"])
        self._test_equal_value("INTERVAL MONTH", ["'1y 10m'"])

    def test_UUID_type(self):
        self.require_protocol_at_least(3 << 16 | 8)
        self._test_equal_value("UUID", ["'00010203-0405-0607-0809-0a0b0c0d0e0f'", "'123e4567-e89b-12d3-a456-426655440a00'"])

    def test_binary_type(self):
        self._test_equal_value("BINARY(2)", [u"'\303\261'"])
        self._test_equal_value("VARBINARY", [u"'\303\261'"])
        self._test_equal_value("LONG VARBINARY", [u"'\303\261\303\260'"])

    def test_array_type(self):
        self._test_equal_value("ARRAY[INT]", ["ARRAY[1,2,3]"])
        self._test_equal_value("ARRAY[ARRAY[INT]]", ["ARRAY[ARRAY[1,2],ARRAY[3,4]]"])

    def test_set_type(self):
        self._test_equal_value("SET[INT]", ["SET[1,2,3]"])

    def test_row_type(self):
        self._test_equal_value("ROW(name varchar, age int, c ARRAY[INT])", ["ROW('Amy',25,ARRAY[1,2,3])"])


exec(DataTransferFormatTestCase.createPrepStmtClass())
