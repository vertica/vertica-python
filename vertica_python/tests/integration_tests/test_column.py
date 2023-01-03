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


class ColumnTestCase(VerticaPythonIntegrationTestCase):
    def test_column_names_query(self):
        columns = ['isocode', 'name', u'\uFF04']

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(u"""
                SELECT 'US' AS {0}, 'United States' AS {1}, 'USD' AS {2}
                UNION ALL SELECT 'CA', 'Canada', 'CAD'
                UNION ALL SELECT 'MX', 'Mexico', 'MXN' """.format(*columns))
            description = cur.description

        self.assertListEqual([d.name for d in description], columns)

    def test_column_description(self):
        type_descriptions = [
            ['boolVal', 5, 1, 1, None, None, True],
            ['intVal', 6, 20, 8, None, None, True],
            ['floatVal', 7, 22, 8, None, None, True],
            ['charVal', 8, 1, -1, None, None, True],
            ['varCharVal', 9, 128, -1, None, None, True],
            ['dateVal', 10, 13, 8, None, None, True],
            ['timestampVal', 12, 29, 8, 6, None, True],
            ['timestampTZVal', 13, 35, 8, 6, None, True],
            ['intervalVal', 14, 24, 8, 4, None, True],
            ['intervalYMVal', 114, 22, 8, 0, None, True],
            ['timeVal', 11, 15, 8, 6, None, True],
            ['timeTZVal', 15, 21, 8, 6, None, True],
            ['varBinVal', 17, 80, -1, None, None, True],
            ['uuidVal', 20, 36, 16, None, None, True],
            ['lVarCharVal', 115, 65536, -1, None, None, True],
            ['lVarBinaryVal', 116, 65536, -1, None, None, True],
            ['binaryVal', 117, 1, -1, None, None, True],
            ['numericVal', 16, 1002, -1, 1000, 18, True]]

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS full_type_tbl")
            cur.execute("""CREATE TABLE full_type_tbl(
                boolVal BOOLEAN,
                intVal INT,
                floatVal FLOAT,
                charVal CHAR,
                varCharVal VARCHAR(128),
                dateVal DATE,
                timestampVal TIMESTAMP,
                timestampTZVal TIMESTAMPTZ,
                intervalVal INTERVAL DAY TO SECOND(4),
                intervalYMVal INTERVAL YEAR TO MONTH,
                timeVal TIME,
                timeTZVal TIMETZ,
                varBinVal VARBINARY,
                uuidVal UUID,
                lVarCharVal LONG VARCHAR(65536),
                lVarBinaryVal LONG VARBINARY(65536),
                binaryVal BINARY,
                numericVal NUMERIC(1000,18))""")
            cur.execute("SELECT * FROM full_type_tbl")
            self.assertListOfListsEqual([list(i) for i in cur.description], type_descriptions)
            cur.execute("DROP TABLE IF EXISTS full_type_tbl")


exec(ColumnTestCase.createPrepStmtClass())
