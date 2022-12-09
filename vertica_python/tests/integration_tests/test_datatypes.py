# Copyright (c) 2018-2022 Micro Focus or one of its affiliates.
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

from datetime import date, datetime, time
from dateutil.relativedelta import relativedelta
from dateutil.tz import tzoffset
from decimal import Decimal
from uuid import UUID

from .base import VerticaPythonIntegrationTestCase


class TypeTestCase(VerticaPythonIntegrationTestCase):
    def test_decimal_query(self):
        value = Decimal(0.42)
        query = "SELECT {0}::numeric".format(value)
        res = self._query_and_fetchone(query)
        self.assertAlmostEqual(res[0], value)

    def test_boolean_query__true(self):
        value = True
        query = "SELECT {0}::boolean".format(value)
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], value)

    def test_boolean_query__false(self):
        value = False
        query = "SELECT {0}::boolean".format(value)
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], value)

    def test_uuid_query(self):
        self.require_protocol_at_least(3 << 16 | 8)
        value = UUID('00010203-0405-0607-0809-0a0b0c0d0e0f')
        query = "SELECT '{0}'::uuid".format(value)
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], value)


exec(TypeTestCase.createPrepStmtClass())


class ComplexTypeTestCase(VerticaPythonIntegrationTestCase):
    def setUp(self):
        super(ComplexTypeTestCase, self).setUp()
        self.require_protocol_at_least(3 << 16 | 12)

    def test_1DArray_boolean_type(self):
        query = "SELECT ARRAY['t', 'f', null]::ARRAY[BOOL], ARRAY[]::ARRAY[BOOL], null::ARRAY[BOOL]"
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], [True, False, None])
        self.assertEqual(res[1], [])
        self.assertEqual(res[2], None)

    def test_1DArray_integer_type(self):
        query = "SELECT ARRAY[-500, 0, null, 500]::ARRAY[INT], ARRAY[]::ARRAY[INT], null::ARRAY[INT]"
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], [-500, 0, None, 500])
        self.assertEqual(res[1], [])
        self.assertEqual(res[2], None)

    def test_1DArray_float_type(self):
        query = ("SELECT ARRAY['Infinity'::float, '-Infinity'::float, null, -1.234, 0, 1.23456e-18]::ARRAY[FLOAT],"
                 " ARRAY[]::ARRAY[FLOAT], null::ARRAY[FLOAT]")
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], [float('Inf'), float('-Inf'), None, -1.234, 0.0, 1.23456e-18])
        self.assertEqual(res[1], [])
        self.assertEqual(res[2], None)

    # def test_1DArray_numeric_type(self):
    #     self._test_equal_value("NUMERIC", ["0", "-1.1", "1234567890123456789.0123456789"])
    #     self._test_equal_value("DECIMAL", ["123456789.98765"])

    # def test_1DArray_char_type(self):
    #     self._test_equal_value("CHAR(8)", [u"'\u16b1'"])
    #     self._test_equal_value("VARCHAR", [u"'foo\u16b1'"])
    #     self._test_equal_value("LONG VARCHAR", [u"'foo \u16b1 bar'"])

    def test_1DArray_date_type(self):
        query = "SELECT ARRAY['2021-06-10', null, '0221-05-02']::ARRAY[DATE], ARRAY[]::ARRAY[DATE], null::ARRAY[DATE]"
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], [date(2021, 6, 10), None, date(221, 5, 2)])
        self.assertEqual(res[1], [])
        self.assertEqual(res[2], None)

    def test_1DArray_time_type(self):
        query = "SELECT ARRAY['00:00:00.00', null, '22:36:33.123956']::ARRAY[TIME(3)], ARRAY[]::ARRAY[TIME(4)], null::ARRAY[TIME]"
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], [time(0, 0, 0), None, time(22, 36, 33, 124000)])
        self.assertEqual(res[1], [])
        self.assertEqual(res[2], None)

    def test_1DArray_timetz_type(self):
        query = "SELECT ARRAY['22:36:33.12345+0630', null, '800-02-03 22:36:33.123456 America/Cayman']::ARRAY[TIMETZ(3)], ARRAY[]::ARRAY[TIMETZ(4)], null::ARRAY[TIMETZ]"
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], [time(22, 36, 33, 123000, tzinfo=tzoffset(None, 23400)), None, 
                                  time(22, 36, 33, 123000, tzinfo=tzoffset(None, -19176))])
        self.assertEqual(res[1], [])
        self.assertEqual(res[2], None)

    # def test_1DArray_timestamp_type(self):
    # def test_1DArray_timestamptz_type(self):
    #     self._test_equal_value("TIMESTAMP", ["'276-12-1 11:22:33'", "'2001-12-01 00:30:45.087'"])
    #     self._test_equal_value("TIMESTAMPTZ(4)", ["'1582-09-24 00:30:45.087-08'", "'0001-1-1 11:22:33'", "'2020-12-31 10:43:09.05'"])

    # def test_1DArray_interval_type(self):
    #     self._test_equal_value("INTERVAL DAY TO SECOND", ["'1 02:03:04.0005'", "'1 02:03:04'", "'02:03:04.0005'"])
    #     self._test_equal_value("INTERVAL DAY TO MINUTE", ["'1 02:03'"])
    #     self._test_equal_value("INTERVAL DAY TO HOUR", ["'1 22'"])
    #     self._test_equal_value("INTERVAL DAY", ["'132'"])
    #     self._test_equal_value("INTERVAL HOUR TO SECOND", ["'02:03:04'"])
    #     self._test_equal_value("INTERVAL HOUR TO MINUTE", ["'02:03'"])
    #     self._test_equal_value("INTERVAL HOUR", ["'02'"])
    #     self._test_equal_value("INTERVAL MINUTE TO SECOND", ["'00:04.0005'", "'03:04'"])
    #     self._test_equal_value("INTERVAL MINUTE", ["'03'"])
    #     self._test_equal_value("INTERVAL SECOND", ["'216901.24'", "'216901'"])

    def test_1DArray_UUID_type(self):
        query = "SELECT ARRAY['00010203-0405-0607-0809-0a0b0c0d0e0f', null]::ARRAY[UUID], ARRAY[]::ARRAY[UUID], null::ARRAY[UUID]"
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], [UUID('00010203-0405-0607-0809-0a0b0c0d0e0f'), None])
        self.assertEqual(res[1], [])
        self.assertEqual(res[2], None)


    # def test_1DArray_binary_type(self):
    #     self._test_equal_value("BINARY(2)", [u"'\303\261'"])
    #     self._test_equal_value("VARBINARY", [u"'\303\261'"])
    #     self._test_equal_value("LONG VARBINARY", [u"'\303\261\303\260'"])

    # def test_NDArray_type(self):
    #     self._test_equal_value("ARRAY[INT]", ["ARRAY[1,2,3]"])
    #     self._test_equal_value("ARRAY[ARRAY[INT]]", ["ARRAY[ARRAY[1,2],ARRAY[3,4]]"])

    # def test_set_type(self):
    #     self._test_equal_value("SET[INT]", ["SET[1,2,3]"])

    # def test_row_type(self):
    #     self._test_equal_value("ROW(name varchar, age int, c ARRAY[INT])", ["ROW('Amy',25,ARRAY[1,2,3])"])


exec(ComplexTypeTestCase.createPrepStmtClass())
