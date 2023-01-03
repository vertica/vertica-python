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

from datetime import date, datetime, time
from dateutil.relativedelta import relativedelta
from dateutil.tz import tzoffset
from decimal import Decimal
from io import open
from uuid import UUID
import logging
import os
import pytest
import re
import shutil
import sys
import tempfile

from parameterized import parameterized

from .base import VerticaPythonIntegrationTestCase
from ... import errors

"""
There are a couple of testcases in this file, they are
1. CursorTestCase:
        general cursor tests, not sensitive to query protocols.
2. SimpleQueryTestCase:
        simple query protocol tests in execute().
3. SimpleQueryExecutemanyTestCase:
        simple query protocol tests in executemany().
4. PreparedStatementTestCase:
        prepared statements tests in both execute() and executemany().

Different query protocols use different paramstyles:
    - simple query protocol: 'named' and 'format' paramstyles
    - extended query protocol (prepared statements): 'qmark' paramstyle
"""


class CursorTestCase(VerticaPythonIntegrationTestCase):
    def setUp(self):
        super(CursorTestCase, self).setUp()
        self._table = 'cursor_test'
        self._init_table()

    def _init_table(self):
        with self._connect() as conn:
            cur = conn.cursor()
            # clean old table
            cur.execute("DROP TABLE IF EXISTS {0}".format(self._table))

            # create test table
            cur.execute("""CREATE TABLE {0} (
                                a INT,
                                b VARCHAR(32)
                           )
                        """.format(self._table))

    def tearDown(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS {0}".format(self._table))
        super(CursorTestCase, self).tearDown()

    def test_multi_inserts_and_transaction(self):
        with self._connect() as conn1, self._connect() as conn2:
            cur1 = conn1.cursor()
            cur2 = conn2.cursor()

            # insert data without a commit
            cur1.execute("INSERT INTO {0} (a, b) VALUES (2, 'bb')".format(self._table))

            # verify we can see it from this cursor
            cur1.execute("SELECT a, b FROM {0} WHERE a = 2".format(self._table))
            res_from_cur_1_before_commit = cur1.fetchall()
            self.assertListOfListsEqual(res_from_cur_1_before_commit, [[2, 'bb']])

            # verify we cant see it from other cursor
            cur2.execute("SELECT a, b FROM {0} WHERE a = 2".format(self._table))

            res_from_cur2_before_commit = cur2.fetchall()
            self.assertListOfListsEqual(res_from_cur2_before_commit, [])

            # insert more data then commit
            cur1.execute("INSERT INTO {0} (a, b) VALUES (3, 'cc')".format(self._table))
            cur1.execute("COMMIT")

            # verify we can see it from this cursor
            cur1.execute(
                "SELECT a, b FROM {0} WHERE a = 2 OR a = 3 ORDER BY a".format(self._table))
            res_from_cur1_after_commit = cur1.fetchall()
            self.assertListOfListsEqual(res_from_cur1_after_commit, [[2, 'bb'], [3, 'cc']])

            # verify we can see it from other cursor
            cur2.execute(
                "SELECT a, b FROM {0} WHERE a = 2 OR a = 3 ORDER BY a".format(self._table))
            res_from_cur2_after_commit = cur2.fetchall()
            self.assertListOfListsEqual(res_from_cur2_after_commit, [[2, 'bb'], [3, 'cc']])

    def test_conn_commit(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("INSERT INTO {0} (a, b) VALUES (5, 'cc')".format(self._table))
            conn.commit()

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT a, b FROM {0} WHERE a = 5".format(self._table))
            res = cur.fetchall()

        self.assertListOfListsEqual(res, [[5, 'cc']])

    def test_delete(self):
        with self._connect() as conn:
            cur = conn.cursor()

            cur.execute("INSERT INTO {0} (a, b) VALUES (5, 'cc')".format(self._table))
            self.assertEqual(cur.rowcount, -1)
            update_res = cur.fetchall()
            self.assertListOfListsEqual(update_res, [[1]])
            conn.commit()

            # validate delete count
            cur.execute("DELETE FROM {0} WHERE a = 5".format(self._table))
            self.assertEqual(cur.rowcount, -1)
            delete_res = cur.fetchall()
            self.assertListOfListsEqual(delete_res, [[1]])
            conn.commit()

            # validate deleted
            cur.execute("SELECT a, b FROM {0} WHERE a = 5".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [])

    def test_update(self):
        with self._connect() as conn:
            cur = conn.cursor()

            cur.execute("INSERT INTO {0} (a, b) VALUES (5, 'cc')".format(self._table))
            # validate insert count
            insert_res = cur.fetchall()
            self.assertListOfListsEqual(insert_res, [[1]], msg='Bad INSERT response')
            conn.commit()

            cur.execute("UPDATE {0} SET b = 'ff' WHERE a = 5".format(self._table))
            # validate update count
            assert cur.rowcount == -1
            update_res = cur.fetchall()
            self.assertListOfListsEqual(update_res, [[1]], msg='Bad UPDATE response')
            conn.commit()

            cur.execute("SELECT a, b FROM {0} WHERE a = 5".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [[5, 'ff']])

    def test_copy_null(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.copy("COPY {0} (a, b) FROM STDIN DELIMITER ','".format(self._table),
                     "1,\n,foo")
            cur.execute("SELECT a, b FROM {0} ORDER BY a ASC".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [[None, 'foo'], [1, None]])

    def test_copy_with_string(self):
        with self._connect() as conn1, self._connect() as conn2:
            cur1 = conn1.cursor()
            cur2 = conn2.cursor()

            cur1.copy("COPY {0} (a, b) FROM STDIN DELIMITER ','".format(self._table),
                      "1,foo\n2,bar")
            # no commit necessary for copy
            cur1.execute("SELECT a, b FROM {0} WHERE a = 1".format(self._table))
            res_from_cur1 = cur1.fetchall()
            self.assertListOfListsEqual(res_from_cur1, [[1, 'foo']])

            cur2.execute("SELECT a, b FROM {0} WHERE a = 2".format(self._table))
            res_from_cur2 = cur2.fetchall()
            self.assertListOfListsEqual(res_from_cur2, [[2, 'bar']])

    # integration test for #345
    def test_copy_with_file_like_object(self):
        class FileWrapper:
            read = property(lambda self: self.file.read)
            seek = property(lambda self: self.file.seek)
            write = property(lambda self: self.file.write)

            def __init__(self, file):
                self.file = file

        with tempfile.TemporaryFile() as f, self._connect() as conn:
            wrapped_f = FileWrapper(f)  # object with a read() method
            wrapped_f.write(b"1,foo\n2,bar")
            # move rw pointer to top of file
            wrapped_f.seek(0)

            cur = conn.cursor()
            cur.copy(
                "COPY {0} (a, b) FROM STDIN DELIMITER ','".format(self._table),
                wrapped_f
            )
            cur.execute("SELECT * FROM {0} ORDER BY a ASC".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [[1, 'foo'], [2, 'bar']])

    # integration test for #325
    @parameterized.expand([
        (tempfile.NamedTemporaryFile,),
        (tempfile.SpooledTemporaryFile,),
        (tempfile.TemporaryFile,),
    ])
    def test_copy_with_temporary_file(self, temp_file_type):
        with temp_file_type() as f, self._connect() as conn:
            f.write(b"1,foo\n2,bar")
            f.seek(0)

            cur = conn.cursor()
            cur.copy(
                "COPY {0} (a, b) FROM STDIN DELIMITER ','".format(self._table),
                f,
            )
            cur.execute("SELECT * FROM {0} ORDER BY a ASC".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [[1, 'foo'], [2, 'bar']])

    def test_copy_with_file(self):
        with tempfile.TemporaryFile() as f, self._connect() as conn1, self._connect() as conn2:
            f.write(b"1,foo\n2,bar")
            # move rw pointer to top of file
            f.seek(0)

            cur1 = conn1.cursor()
            cur2 = conn2.cursor()

            cur1.copy("COPY {0} (a, b) FROM STDIN DELIMITER ','".format(self._table),
                      f)
            # no commit necessary for copy
            cur1.execute("SELECT a, b FROM {0} WHERE a = 1".format(self._table))
            res_from_cur1 = cur1.fetchall()
            self.assertListOfListsEqual(res_from_cur1, [[1, 'foo']])

            cur2.execute("SELECT a, b FROM {0} WHERE a = 2".format(self._table))
            res_from_cur2 = cur2.fetchall()
            self.assertListOfListsEqual(res_from_cur2, [[2, 'bar']])

    def test_copy_with_closed_file(self):
        with tempfile.TemporaryFile() as f, self._connect() as conn:
            f.write(b"1,foo\n2,bar")
            # move rw pointer to top of file
            f.seek(0)

            cur = conn.cursor()
            f.close()
            with pytest.raises(errors.DataError, match='closed file'):
                cur.copy("COPY {0} (a, b) FROM STDIN DELIMITER ','".format(self._table),
                          f)
            # Must not close the cursor object and able to successfully run queries
            cur.execute("SELECT 1;")
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [[1]])

    # unit test for #78
    def test_copy_with_data_in_buffer(self):
        with self._connect() as conn:
            cur = conn.cursor()

            cur.execute("SELECT 1;")
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [[1]])

            cur.copy("COPY {0} (a, b) FROM STDIN DELIMITER ','".format(self._table),
                     "1,foo\n2,bar")

            cur.execute("SELECT 1;")
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [[1]])

    # unit test for #213
    def test_cmd_after_invalid_copy_stmt(self):
        with self._connect() as conn:
            cur = conn.cursor()

            cur.execute("SELECT 1;")
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [[1]])

            res = [[]]
            try:
                cur.copy("COPY non_existing_tab(a, b) FROM STDIN DELIMITER ','", "FAIL")
            except errors.MissingRelation as e:
                cur.execute("SELECT 1;")
                res = cur.fetchall()

            self.assertListOfListsEqual(res, [[1]])

    # unit test for #213
    def test_cmd_after_rejected_copy_data(self):
        with self._connect() as conn:
            cur = conn.cursor()

            cur.execute("SELECT 1;")
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [[1]])

            res = [[]]
            try:
                cur.copy("COPY {0} (a, b) FROM STDIN DELIMITER ',' ABORT ON ERROR".format(self._table),
                         "FAIL")
            except errors.CopyRejected as e:
                cur.execute("SELECT 1;")
                res = cur.fetchall()

            self.assertListOfListsEqual(res, [[1]])

    def test_with_conn(self):
        with self._connect() as conn:
            cur = conn.cursor()

            cur.execute("INSERT INTO {0} (a, b) VALUES (1, 'aa');".format(self._table))
            conn.commit()
            cur.execute("SELECT a, b FROM {0} WHERE a = 1".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [[1, 'aa']])

    def test_iterator(self):
        with self._connect() as conn:
            cur = conn.cursor()
            values = [[1, 'aa'], [2, 'bb'], [3, 'cc']]

            for n, s in values:
                cur.execute("INSERT INTO {} (a, b) VALUES ({}, '{}')"
                            .format(self._table, n, s))
            conn.commit()

            cur.execute("SELECT a, b FROM {0} ORDER BY a ASC".format(self._table))

            for val, res in zip(sorted(values), cur.iterate()):
                self.assertListEqual(res, val)

            remaining = cur.fetchall()
            self.assertListOfListsEqual(remaining, [])

    def test_mid_iterator_execution(self):
        with self._connect() as conn:
            cur = conn.cursor()
            values = [[1, 'aa'], [2, 'bb'], [3, 'cc']]

            for n, s in values:
                cur.execute("INSERT INTO {} (a, b) VALUES ({}, '{}')"
                            .format(self._table, n, s))
            conn.commit()

            cur.execute("SELECT a, b FROM {0} ORDER BY a ASC".format(self._table))

            for val, res in zip(sorted(values), cur.iterate()):
                self.assertListEqual(res, val)
                break  # stop after one comparison

            # make new query and verify result
            cur.execute("SELECT COUNT(*) FROM {0}".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [[3]])

    def test_query_errors(self):
        with self._connect() as conn:
            cur = conn.cursor()

            # create table syntax error
            with self.assertRaises(errors.VerticaSyntaxError):
                cur.execute("""CREATE TABLE {0}_fail (
                                a INT,
                                b VARCHAR(32),,,
                                );
                            """.format(self._table))

            # select table not found error
            cur.execute("INSERT INTO {0} (a, b) VALUES (1, 'aa');".format(self._table))
            cur.execute("COMMIT;")
            with self.assertRaises(errors.QueryError):
                cur.execute("SELECT * FROM {0}_fail".format(self._table))

            # generate a user-defined error message
            err_msg = 'USER GENERATED ERROR: test error'
            with pytest.raises(errors.QueryError, match=err_msg):
                cur.execute("SELECT THROW_ERROR('test error')")

            # verify cursor still usable after errors
            cur.execute("SELECT a, b FROM {0} WHERE a = 1".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [[1, 'aa']])

    def test_cursor_close_and_reuse(self):
        with self._connect() as conn:
            cur = conn.cursor()

            # insert data
            cur.execute("INSERT INTO {0} (a, b) VALUES (2, 'bb');".format(self._table))
            cur.execute("COMMIT;")

            # (query -> close -> reopen) * 3 times
            for _ in range(3):
                cur.execute("SELECT a, b FROM {0} WHERE a = 2".format(self._table))
                res = cur.fetchall()
                self.assertListOfListsEqual(res, [[2, 'bb']])

                # close and reopen cursor
                cur.close()
                with pytest.raises(errors.InterfaceError, match='Cursor is closed'):
                    cur.execute("SELECT 1;")
                cur = conn.cursor()

    def test_udtype(self):
        poly = "POLYGON ((1 2, 2 3, 3 1, 1 2))"
        line = "LINESTRING (42.1 71, 41.4 70, 41.3 72.9, 42.99 71.46, 44.47 73.21)"
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS {0}".format(self._table))
            cur.execute("CREATE TABLE {} (c1 GEOMETRY(10000), c2 GEOGRAPHY(1000))"
                .format(self._table))
            cur.execute("INSERT INTO {} VALUES (ST_GeomFromText('{}'), ST_GeographyFromText('{}'))"
                .format(self._table, poly, line))
            conn.commit()
            cur.execute("SELECT c1, c2, ST_AsText(c1), ST_AsText(c2) FROM {}".format(self._table))

            res = cur.fetchall()
            self.assertEqual(res[0][2], poly)
            self.assertEqual(res[0][3], line)

            datatype_names = [col.type_name for col in cur.description]
            expected = ['geometry', 'geography', 'Long Varchar', 'Long Varchar']
            self.assertListEqual(datatype_names, expected)

            self.assertEqual(cur.description[0].display_size, 10000)
            self.assertEqual(cur.description[1].display_size, 1000)

    def test_disable_sqldata_converter(self):
        with self._connect() as conn:
            cur = conn.cursor()

            # Default is False
            self.assertFalse(cur.disable_sqldata_converter)

            # Set with attribute setter
            cur.disable_sqldata_converter = True
            self.assertTrue(cur.disable_sqldata_converter)
            cur.execute("INSERT INTO {0} (a, b) VALUES (1, 'aa')".format(self._table))
            cur.execute("INSERT INTO {0} (a, b) VALUES (2, 'bb')".format(self._table))
            conn.commit()
            cur.execute("SELECT a, b FROM {0} ORDER BY a ASC".format(self._table))
            res = cur.fetchall()
            if conn.options['binary_transfer']:
                self.assertListOfListsEqual(res,
                    [[b'\x00\x00\x00\x00\x00\x00\x00\x01', b'aa'], [b'\x00\x00\x00\x00\x00\x00\x00\x02', b'bb']])
            else:
                self.assertListOfListsEqual(res, [[b'1', b'aa'], [b'2', b'bb']])

exec(CursorTestCase.createPrepStmtClass())


class SimpleQueryTestCase(VerticaPythonIntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        super(SimpleQueryTestCase, cls).setUpClass()
        cls._conn_info['use_prepared_statements'] = False
        # Create data files for COPY LOCAL tests
        with tempfile.NamedTemporaryFile(delete=False) as cls._f1:
            cls._f1.write(b"1,foo\n2,bar\nx\xc3\xb1,bla")
        with tempfile.NamedTemporaryFile(delete=False) as cls._f2:
            cls._f2.write(b"4,\n5," + b'a'*10 + b"\n,baz")
        with tempfile.NamedTemporaryFile(delete=False) as cls._f3:
            cls._f3.write(b"10," + b'k'*12 + b"\n11,qux\nxx,corge")
        with tempfile.NamedTemporaryFile(delete=False) as cls._f4:
            cls._f4.write(b"13,flob\nf,quux\n15,xyz")

    @classmethod
    def tearDownClass(cls):
        for f in (cls._f1, cls._f2, cls._f3, cls._f4):
            os.remove(f.name)

    def setUp(self):
        super(SimpleQueryTestCase, self).setUp()
        self._table = 'simplequery_test'
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS {0}".format(self._table))

    def tearDown(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS {0}".format(self._table))
        super(SimpleQueryTestCase, self).tearDown()

    def test_inline_commit(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE {0} (a INT, b VARCHAR)".format(self._table))
            cur.execute("INSERT INTO {0} (a, b) VALUES (1, 'aa'); COMMIT;"
                        .format(self._table))
            cur.execute("SELECT a, b FROM {0} WHERE a = 1".format(self._table))

            # unknown rowcount
            self.assertEqual(cur.rowcount, -1)

            res = cur.fetchall()
            self.assertEqual(cur.rowcount, 1)

            self.assertListOfListsEqual(res, [[1, 'aa']])

    # unit test for #144
    def test_empty_query(self):
        with self._connect() as conn:
            cur = conn.cursor()

            cur.execute("")
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [])

            cur.execute("--select 1")
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [])

            cur.execute("""
                        /*
                        Test block comment
                        */
                        """)
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [])

    # unit test for #175
    def test_datetime_types(self):
        with self._connect() as conn:
            cur = conn.cursor()

            # create test table
            cur.execute("""CREATE TABLE {0} (
                                a INT,
                                b VARCHAR(32),
                                c TIMESTAMP,
                                d DATE,
                                e TIME
                           )
                        """.format(self._table))

            cur.execute("INSERT INTO {0} VALUES (:n, :s, :dt, :d, :t)".format(self._table),
                            {'n': 10, 's': 'aa',
                             'dt': datetime(2018, 9, 7, 15, 38, 19, 769000),
                             'd': date(2018, 9, 7),
                             't': time(13, 50, 9)})
            conn.commit()

            cur.execute("SELECT a, b, c, d, e FROM {0}".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [[10, 'aa',
                                               datetime(2018, 9, 7, 15, 38, 19, 769000),
                                               date(2018, 9, 7), time(13, 50, 9)]])

    def test_binary_types(self):
        with self._connect() as conn:
            cur = conn.cursor()

            # create test table
            cur.execute("""CREATE TABLE {0} (
                                a binary(1),
                                b binary(3),
                                c varbinary
                            )
                        """.format(self._table))

            cur.execute("INSERT INTO {0} VALUES (:b, :s1, :s2)".format(self._table),
                        {'b': b'x', 's1': b'xyz', 's2': 'abcde'})
            conn.commit()

            cur.execute("SELECT a, b, c FROM {0}".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [[b'x', b'xyz', b'abcde']])

    def test_uuid_type(self):
        with self._connect() as conn:
            cur = conn.cursor()
            x = UUID('{00010203-0405-0607-0809-0a0b0c0d0e0f}')
            y = UUID('00000020-0000-0000-0000-100000000000')
            z = UUID('00100010000000000000000000000000')

            # create test table
            cur.execute("CREATE TABLE {0} ( a uuid, b uuid, c uuid )".format(self._table))
            cur.execute("INSERT INTO {0} VALUES (:u1, :u2, :u3)".format(self._table),
                        {'u1': x, 'u2': y, 'u3': z})
            conn.commit()

            cur.execute("SELECT a, b, c FROM {0}".format(self._table))
            res = cur.fetchall()[0]
            self.assertListEqual([str(i) for i in res], [str(x), str(y), str(z)])

    # unit test for #74
    def test_nextset(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1; SELECT 'foo';")

            res1 = cur.fetchall()
            self.assertEqual(cur.rowcount, 1)
            self.assertListOfListsEqual(res1, [[1]])
            self.assertIsNone(cur.fetchone())
            self.assertTrue(cur.nextset())

            res2 = cur.fetchall()
            self.assertEqual(cur.rowcount, 1)
            self.assertListOfListsEqual(res2, [['foo']])
            self.assertIsNone(cur.fetchone())
            self.assertFalse(cur.nextset())

    # unit test for #74
    def test_nextset_with_delete(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE {0} (a INT, b VARCHAR(32))".format(self._table))
            # insert data
            cur.execute("INSERT INTO {0} (a, b) VALUES (1, 'aa')".format(self._table))
            cur.execute("INSERT INTO {0} (a, b) VALUES (2, 'bb')".format(self._table))
            conn.commit()

            cur.execute("""
                          SELECT * FROM {0} ORDER BY a ASC;
                          DELETE FROM {0};
                          SELECT * FROM {0} ORDER BY a ASC;
                        """.format(self._table))

            # check first select results
            res1 = cur.fetchall()
            self.assertListOfListsEqual(res1, [[1, 'aa'], [2, 'bb']])
            self.assertIsNone(cur.fetchone())
            self.assertTrue(cur.nextset())

            # check delete results
            res2 = cur.fetchall()
            self.assertListOfListsEqual(res2, [[2]])
            self.assertIsNone(cur.fetchone())
            self.assertTrue(cur.nextset())

            # check second select results
            res3 = cur.fetchall()
            self.assertListOfListsEqual(res3, [])
            self.assertIsNone(cur.fetchone())
            self.assertFalse(cur.nextset())

    # unit test for #124
    def test_nextset_with_error(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1; SELECT a; SELECT 2")

            # verify data from first query
            res1 = cur.fetchall()
            self.assertListOfListsEqual(res1, [[1]])
            self.assertIsNone(cur.fetchone())

            # second statement results in a query error
            with self.assertRaises(errors.MissingColumn):
                cur.nextset()

    def test_qmark_paramstyle(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE {0} (a INT, b VARCHAR)".format(self._table))
            err_msg = 'not all arguments converted during string formatting'
            values = [1, 'aa']
            with pytest.raises(TypeError, match=err_msg):
                cur.execute("INSERT INTO {} VALUES (?, ?)".format(self._table), values)

            cur.execute("INSERT INTO {} VALUES (?, ?)".format(self._table),
                        values, use_prepared_statements=True)
            cur.execute("SELECT * FROM {}".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [values])

    def test_execute_parameters(self):
        with self._connect() as conn:
            cur = conn.cursor()
            all_chars = u"".join(chr(i) for i in range(1, 128))
            backslash_data = u"\\backslash\\ \\data\\\\"
            cur.execute("SELECT :a, :b", parameters={"a": all_chars, "b": backslash_data})
            self.assertEqual([all_chars, backslash_data], cur.fetchone())

    def test_execute_percent_parameters(self):
        with self._connect() as conn:
            cur = conn.cursor()
            all_chars = u"".join(chr(i) for i in range(1, 128))
            backslash_data = u"\\backslash\\ \\data\\\\"
            cur.execute("SELECT %s, %s", parameters=[all_chars, backslash_data])
            self.assertEqual([all_chars, backslash_data], cur.fetchone())

    def test_disabled_copy_local(self):
        self._conn_info['disable_copy_local'] = True

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE {0} (a INT, b VARCHAR(9))".format(self._table))
            with pytest.raises(errors.InterfaceError, match='disabled'):
                cur.execute(
                    "COPY {} FROM LOCAL '{}','{}' DELIMITER ',' ENFORCELENGTH;"
                    "SELECT 100;"
                    .format(self._table, self._f1.name, self._f2.name)
                )
            self.assertListOfListsEqual(cur.fetchall(), [])
            self.assertFalse(cur.nextset())

            # Must not close the cursor object and able to successfully run queries
            cur.execute("SELECT 1;")
            self.assertListOfListsEqual(cur.fetchall(), [[1]])

    def test_copy_local_stdin_input_options(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE {0} (a INT, b VARCHAR(9))".format(self._table))
            # No STDIN input
            with pytest.raises(ValueError, match='No STDIN source'):
                cur.execute(
                    "COPY {} FROM LOCAL STDIN DELIMITER ',' ENFORCELENGTH"
                    .format(self._table))
            with pytest.raises(ValueError, match='No STDIN source'):
                cur.execute(
                    "COPY {} FROM LOCAL STDIN DELIMITER ',' ENFORCELENGTH"
                    .format(self._table), copy_stdin=[])
            # Invalid STDIN input
            with pytest.raises(TypeError, match='file-like object'):
                cur.execute(
                    "COPY {} FROM LOCAL STDIN DELIMITER ',' ENFORCELENGTH"
                    .format(self._table), copy_stdin='Not file-like')
            with pytest.raises(TypeError, match='file-like object'):
                cur.execute(
                    "COPY {} FROM LOCAL STDIN DELIMITER ',' ENFORCELENGTH"
                    .format(self._table), copy_stdin=['Not file-like'])
            # A file-like object as STDIN input
            fs = open(self._f1.name)
            cur.execute(
                "COPY {} FROM LOCAL STDIN DELIMITER ',' ENFORCELENGTH"
                .format(self._table), copy_stdin=fs)
            res1 = cur.fetchall()
            self.assertListOfListsEqual(res1, [[2]])
            fs.close()

    def test_copy_local_stdin_multistat(self):
        # Define paths to rejected files
        tmpdir = os.path.dirname(self._f1.name)
        rejdir = os.path.join(tmpdir, 'copylocal1')
        rej1 = os.path.join(rejdir, 'copy_rej.txt')
        except1 = os.path.join(rejdir, 'copy_exception.txt')
        if os.path.isdir(rejdir):
            shutil.rmtree(rejdir)
        rej2 = os.path.join(tmpdir, 'copy_rej2.txt')
        except2 = os.path.join(tmpdir, 'copy_exception2.txt')
        for f in (rej2, except2):
            if os.path.isfile(f):
                os.remove(f)

        # Execute the COPY LOCAL statement as the first and later statement
        # within a query containing multiple statements
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE {0} (a INT, b VARCHAR(9))".format(self._table))
            # Feed STDIN
            f1 = open(self._f1.name)
            f2 = open(self._f3.name)
            cur.execute(
                "COPY {} FROM LOCAL STDIN DELIMITER ',' ENFORCELENGTH"
                " REJECTED DATA '{}' EXCEPTIONS '{}';"
                "SELECT 100;"
                "COPY {} FROM LOCAL STDIN DELIMITER ',' ENFORCELENGTH"
                " REJECTED DATA '{}' EXCEPTIONS '{}';"
                "SELECT 200;"
                .format(self._table, rej1, except1,
                        self._table, rej2, except2),
                copy_stdin=[f1, f2]
            )

            res1 = cur.fetchall()
            self.assertListOfListsEqual(res1, [[2]])
            self.assertTrue(cur.nextset())
            f1.close()

            res2 = cur.fetchall()
            self.assertListOfListsEqual(res2, [[100]])
            self.assertTrue(cur.nextset())

            res3 = cur.fetchall()
            self.assertListOfListsEqual(res3, [[1]])
            self.assertTrue(cur.nextset())
            f2.close()

            res4 = cur.fetchall()
            self.assertListOfListsEqual(res4, [[200]])
            self.assertFalse(cur.nextset())

            # There should be no hang/error in the next execution call
            cur.execute("SELECT * FROM {0} ORDER BY a ASC".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [[1, 'foo'], [2, 'bar'], [11, 'qux']])

        # Check rejected files
        with open(rej1, 'r', encoding='utf-8') as f:
            self.assertEqual(f.read(), u'x\u00f1,bla\n')
        with open(except1, 'r', encoding='utf-8') as f:
            content = f.read()
            self.assertTrue(u"Invalid integer format 'x\u00f1' for column 1 (a)" in content)
        with open(rej2, 'r', encoding='utf-8') as f:
            self.assertEqual(f.read(), u'10,kkkkkkkkkkkk\nxx,corge\n')
        with open(except2, 'r', encoding='utf-8') as f:
            content = f.read()
            self.assertTrue(u"The 12-byte value is too long for type Varchar(9), column 2 (b)" in content)
            self.assertTrue(u"Invalid integer format 'xx' for column 1 (a)" in content)

        # Delete data files
        try:
            for f in (rej2, except2):
                os.remove(f)
            shutil.rmtree(rejdir)
        except Exception:
            pass

    def test_copy_local_file_not_exist(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE {0} (a INT, b VARCHAR(9))".format(self._table))
            with pytest.raises(OSError, match='not_exist.file does not exist'):
                cur.execute(
                    "COPY {} FROM LOCAL '{}','not_exist.file' DELIMITER ',' ENFORCELENGTH"
                    .format(self._table, self._f1.name))
            self.assertListOfListsEqual(cur.fetchall(), [])
            self.assertFalse(cur.nextset())

            # Must not close the cursor object and able to successfully run queries
            cur.execute("SELECT 1;")
            self.assertListOfListsEqual(cur.fetchall(), [[1]])

    def test_copy_local_glob(self):
        suffix = ".copy_glob_test"
        files = (self._f1.name, self._f2.name, self._f3.name, self._f4.name)
        fdir = os.path.dirname(self._f1.name)
        for f in files:
            shutil.copy(f, f + suffix)
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE {0} (a INT, b VARCHAR(9))".format(self._table))
            cur.execute(
                "COPY {} FROM LOCAL '{}' DELIMITER ',' ENFORCELENGTH"
                .format(self._table, os.path.join(fdir, '*' + suffix)))
            self.assertListOfListsEqual(cur.fetchall(), [[7]])
        for f in files:
            os.remove(f + suffix)

    @parameterized.expand([(True,), (False,)])
    def test_copy_local_file_multistat(self, fetch_results):
        # Define paths to rejected files
        tmpdir = os.path.dirname(self._f1.name)
        rejdir = os.path.join(tmpdir, 'copylocal1')
        rej1 = os.path.join(rejdir, 'copy_rej.txt')
        except1 = os.path.join(rejdir, 'copy_exception.txt')
        if os.path.isdir(rejdir):
            shutil.rmtree(rejdir)
        rej2 = os.path.join(tmpdir, 'copy_rej2.txt')
        except2 = os.path.join(tmpdir, 'copy_exception2.txt')
        for f in (rej2, except2):
            if os.path.isfile(f):
                os.remove(f)

        # Execute the COPY LOCAL statement as the first and later statement
        # within a query containing multiple statements
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE {0} (a INT, b VARCHAR(9))".format(self._table))
            cur.execute(
                "COPY {} FROM LOCAL '{}','{}' DELIMITER ',' ENFORCELENGTH"
                " REJECTED DATA '{}' EXCEPTIONS '{}';"
                "SELECT 100;"
                "COPY {} FROM LOCAL '{}','{}' DELIMITER ',' ENFORCELENGTH"
                " REJECTED DATA '{}' EXCEPTIONS '{}';"
                "SELECT 200;"
                .format(self._table, self._f1.name, self._f2.name, rej1, except1,
                        self._table, self._f3.name, self._f4.name, rej2, except2)
            )

            if fetch_results:
                res1 = cur.fetchall()
                self.assertListOfListsEqual(res1, [[4]])
                self.assertTrue(cur.nextset())

                res2 = cur.fetchall()
                self.assertListOfListsEqual(res2, [[100]])
                self.assertTrue(cur.nextset())

                res3 = cur.fetchall()
                self.assertListOfListsEqual(res3, [[3]])
                self.assertTrue(cur.nextset())

                res4 = cur.fetchall()
                self.assertListOfListsEqual(res4, [[200]])
                self.assertFalse(cur.nextset())

            # There should be no hang/error in the next execution call
            cur.execute("SELECT * FROM {0} ORDER BY a ASC".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [[None, 'baz'], [1, 'foo'], [2, 'bar'],
                    [4, None], [11, 'qux'], [13, 'flob'], [15, 'xyz']])

        # Check rejected files
        with open(rej1, 'r', encoding='utf-8') as f:
            self.assertEqual(f.read(), u'x\u00f1,bla\n5,aaaaaaaaaa\n')
        with open(except1, 'r', encoding='utf-8') as f:
            content = f.read()
            self.assertTrue(u"Invalid integer format 'x\u00f1' for column 1 (a)" in content)
            self.assertTrue(u"The 10-byte value is too long for type Varchar(9), column 2 (b)" in content)
        with open(rej2, 'r', encoding='utf-8') as f:
            self.assertEqual(f.read(), u'10,kkkkkkkkkkkk\nxx,corge\nf,quux\n')
        with open(except2, 'r', encoding='utf-8') as f:
            content = f.read()
            self.assertTrue(u"The 12-byte value is too long for type Varchar(9), column 2 (b)" in content)
            self.assertTrue(u"Invalid integer format 'xx' for column 1 (a)" in content)
            self.assertTrue(u"Invalid integer format 'f' for column 1 (a)" in content)

        # Delete files
        try:
            for f in (rej2, except2):
                os.remove(f)
            shutil.rmtree(rejdir)
        except Exception:
            pass

    def test_copy_local_returnrejected(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE {0} (a INT, b VARCHAR(9))".format(self._table))
            msg = "COPY LOCAL does not support rejected row numbers with exceptions or rejected data options"
            with pytest.raises(errors.QueryError, match=msg):
                cur.execute(
                    "COPY {} FROM LOCAL '{}','{}' DELIMITER ',' ENFORCELENGTH"
                    " RETURNREJECTED"
                    " REJECTED DATA 'copy_rej.txt' EXCEPTIONS 'copy_exp.txt'"
                    .format(self._table, self._f1.name, self._f2.name))
            # Rejected row numbers write to log file
            cur.execute(
                "COPY {} FROM LOCAL '{}','{}' DELIMITER ',' ENFORCELENGTH"
                " RETURNREJECTED"
                .format(self._table, self._f1.name, self._f2.name))
            self.assertListOfListsEqual(cur.fetchall(), [[4]])
            cur.execute("SELECT * FROM {0} ORDER BY a ASC".format(self._table))
            self.assertListOfListsEqual(cur.fetchall(), [[None, 'baz'], [1, 'foo'], [2, 'bar'], [4, None]])

    def test_copy_local_rejected_as_table(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE {0} (a INT, b VARCHAR(9))".format(self._table))
            cur.execute("DROP TABLE IF EXISTS test_loader_rejects CASCADE")
            cur.execute(
                "COPY {} FROM LOCAL '{}','{}' DELIMITER ',' ENFORCELENGTH"
                " REJECTED DATA AS TABLE test_loader_rejects"
                .format(self._table, self._f1.name, self._f2.name))
            self.assertListOfListsEqual(cur.fetchall(), [[4]])

            cur.execute("SELECT rejected_data, rejected_reason FROM test_loader_rejects ORDER BY row_number ASC")
            self.assertListOfListsEqual(cur.fetchall(),
                [['5,aaaaaaaaaa', 'The 10-byte value is too long for type Varchar(9), column 2 (b)'],
                 [u'x\u00f1,bla', u"Invalid integer format 'x\u00f1' for column 1 (a)"]])

            cur.execute("SELECT * FROM {0} ORDER BY a ASC".format(self._table))
            self.assertListOfListsEqual(cur.fetchall(), [[None, 'baz'], [1, 'foo'], [2, 'bar'], [4, None]])

            cur.execute("DROP TABLE IF EXISTS test_loader_rejects CASCADE")

    def test_copy_local_abort_on_error(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE {0} (a INT, b VARCHAR(9))".format(self._table))
            msg = r"The 12-byte value is too long for type Varchar\(9\), column 2 \(b\)"

            # COPY LOCAL FILE
            with pytest.raises(errors.CopyRejected, match=msg):
                cur.execute(
                    "COPY {} FROM LOCAL '{}' DELIMITER ',' ENFORCELENGTH ABORT ON ERROR"
                    .format(self._table, self._f3.name))
            # Must not close the cursor object and able to successfully run queries
            cur.execute("SELECT 1;")
            self.assertListOfListsEqual(cur.fetchall(), [[1]])

            # COPY LOCAL STDIN
            with pytest.raises(errors.CopyRejected, match=msg):
                cur.execute(
                    "COPY {} FROM LOCAL STDIN DELIMITER ',' ENFORCELENGTH ABORT ON ERROR"
                    .format(self._table),
                    copy_stdin=[open(self._f3.name)])
            # Must not close the cursor object and able to successfully run queries
            cur.execute("SELECT 1;")
            self.assertListOfListsEqual(cur.fetchall(), [[1]])


class SimpleQueryExecutemanyTestCase(VerticaPythonIntegrationTestCase):
    def setUp(self):
        super(SimpleQueryExecutemanyTestCase, self).setUp()
        self._table = 'executemany_test'
        self._init_table()

    def _init_table(self):
        with self._connect() as conn:
            cur = conn.cursor()
            # clean old table
            cur.execute("DROP TABLE IF EXISTS {0}".format(self._table))

            # create test table
            cur.execute("""CREATE TABLE {0} (
                                a INT,
                                b VARCHAR(32)
                           )
                        """.format(self._table))

    def tearDown(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS {0}".format(self._table))
        super(SimpleQueryExecutemanyTestCase, self).tearDown()

    def _test_executemany(self, table, seq_of_values):
        with self._connect() as conn:
            cur = conn.cursor()

            cur.executemany("INSERT INTO {0} (a, b) VALUES (%s, %s)".format(table),
                            seq_of_values)
            conn.commit()

            cur.execute("SELECT * FROM {0} ORDER BY a ASC, b ASC".format(table))

            # check first select results
            res1 = cur.fetchall()
            seq_of_values_to_compare = sorted([list(values) for values in seq_of_values])
            self.assertListOfListsEqual(res1, seq_of_values_to_compare)
            self.assertIsNone(cur.fetchone())

    def test_executemany(self):
        self._test_executemany(self._table, [(i, chr(i)) for i in range(0, 128)])

    def test_executemany_quoted_path(self):
        table = '.'.join(['"{}"'.format(s.strip('"')) for s in self._table.split('.')])
        self._test_executemany(table, [(1, 'aa'), (2, 'bb')])

    def test_executemany_utf8(self):
        self._test_executemany(self._table, [(1, u'a\xfc'), (2, u'bb')])

    # test for #292
    def test_executemany_autocommit(self):
        with self._connect() as conn:
            cur = conn.cursor()
            conn.autocommit = False
            cur.execute('BEGIN')
            cur.executemany("INSERT INTO {0} (a, b) VALUES (%s, %s)".format(self._table),
                            ((None, 'foo'), [2, None], [3, 'bar']))
            cur.execute('ROLLBACK')

            cur.execute("SELECT count(*) FROM {0}".format(self._table))
            res = cur.fetchone()[0]
            self.assertEqual(res, 0)

    def test_executemany_null(self):
        seq_of_values_1 = ((None, 'foo'), [2, None])
        seq_of_values_2 = ({'a': None, 'b': 'bar'}, {'a': 4, 'b': None})
        seq_of_values_to_compare = [[None, 'bar'], [None, 'foo'], [2, None], [4, None]]
        with self._connect() as conn:
            cur = conn.cursor()

            cur.executemany("INSERT INTO {0} (a, b) VALUES (%s, %s)".format(self._table),
                            seq_of_values_1)
            conn.commit()
            cur.executemany("INSERT INTO {0} (a, b) VALUES (:a, :b)".format(self._table),
                            seq_of_values_2)
            conn.commit()

            cur.execute("SELECT * FROM {0} ORDER BY a ASC, b ASC".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, seq_of_values_to_compare)
            self.assertIsNone(cur.fetchone())

    def test_executemany_empty(self):
        err_msg = "executemany is implemented for simple INSERT statements only"
        with self._connect() as conn:
            cur = conn.cursor()
            with pytest.raises(NotImplementedError, match=err_msg):
                cur.executemany("", [])


class PreparedStatementTestCase(VerticaPythonIntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        super(PreparedStatementTestCase, cls).setUpClass()
        cls._conn_info['use_prepared_statements'] = True

    def setUp(self):
        super(PreparedStatementTestCase, self).setUp()
        self._table = 'preparedstmt_test'
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS {0}".format(self._table))

    def tearDown(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS {0}".format(self._table))
        super(PreparedStatementTestCase, self).tearDown()

    def test_empty_statement(self):
        with self._connect() as conn:
            cur = conn.cursor()
            err_msg = 'The statement being prepared is empty'
            with pytest.raises(errors.EmptyQueryError, match=err_msg):
                cur.execute("")
            with pytest.raises(errors.EmptyQueryError, match=err_msg):
                cur.executemany("", [()])
            with pytest.raises(errors.EmptyQueryError, match=err_msg):
                cur.execute("--select 1")
            with pytest.raises(errors.EmptyQueryError, match=err_msg):
                cur.execute("""
                        /*
                        Test block comment
                        */
                        """)

    def test_compound_statement(self):
        with self._connect() as conn:
            cur = conn.cursor()
            query = "select 1; select 2; select 3;"
            err_msg = 'Cannot insert multiple commands into a prepared statement'
            with pytest.raises(errors.VerticaSyntaxError, match=err_msg):
                cur.execute(query)

    def test_num_of_parameters(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE {} (a int, b varchar)".format(self._table))
            err_msg = 'Invalid number of parameters'

            # The number of parameters is less than columns of table
            with pytest.raises(ValueError, match=err_msg):
                cur.execute("INSERT INTO {} VALUES (?,?)".format(self._table))

            # The number of parameters is greater than columns of table
            with pytest.raises(ValueError, match=err_msg):
                cur.execute("INSERT INTO {} VALUES (?,?)".format(self._table), [1, 'foo', 2])

            # The number of parameters is equal to columns of table
            values = [1, 'varchar']
            cur.execute("INSERT INTO {} VALUES (?,?)".format(self._table), values)
            conn.commit()
            cur.execute("SELECT * FROM {}".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [values])

    def test_format_paramstyle(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE {} (a int, b varchar)".format(self._table))
            err_msg = 'Syntax error at or near "%"'
            values = [1, 'varchar']
            with pytest.raises(errors.VerticaSyntaxError, match=err_msg):
                cur.execute("INSERT INTO {} VALUES (%s, %s)".format(self._table), values)

            cur.execute("INSERT INTO {} VALUES (%s, %s)".format(self._table),
                        values, use_prepared_statements=False)
            conn.commit()
            cur.execute("SELECT * FROM {}".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [values])

    def test_named_paramstyle(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE {} (a int, b varchar)".format(self._table))
            err_msg = 'Execute parameters should be a list/tuple'
            values = {'a': 1, 'b': 'varchar'}
            with pytest.raises(TypeError, match=err_msg):
                cur.execute("INSERT INTO {} VALUES (:a, :b)".format(self._table), values)

            cur.execute("INSERT INTO {} VALUES (:a, :b)".format(self._table),
                        values, use_prepared_statements=False)
            conn.commit()
            cur.execute("SELECT * FROM {}".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [[1, 'varchar']])

    def test_executemany(self):
        values = ((None, 'foo'), [1, 'aa'], (2, None), [2, u'a\xfc'])
        expected = [[None, 'foo'], [1, 'aa'], [2, None], [2, u'a\xfc']]
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE {} (a int, b varchar)".format(self._table))
            cur.executemany("INSERT INTO {} VALUES (?, ?)".format(self._table), values)

            self.assertListOfListsEqual(cur.fetchall(), [[1]])
            self.assertIsNone(cur.fetchone())
            self.assertTrue(cur.nextset())

            self.assertListOfListsEqual(cur.fetchall(), [[1]])
            self.assertIsNone(cur.fetchone())
            self.assertTrue(cur.nextset())

            self.assertListOfListsEqual(cur.fetchall(), [[1]])
            self.assertIsNone(cur.fetchone())
            self.assertTrue(cur.nextset())

            self.assertListOfListsEqual(cur.fetchall(), [[1]])
            self.assertIsNone(cur.fetchone())
            self.assertFalse(cur.nextset())

            cur.executemany("SELECT * FROM {} WHERE a=? ORDER BY b"
                            .format(self._table), [[1], [2], [3]])
            self.assertListOfListsEqual(cur.fetchall(), [[1, 'aa']])
            self.assertIsNone(cur.fetchone())
            self.assertTrue(cur.nextset())

            self.assertListOfListsEqual(cur.fetchall(), [[2, u'a\xfc'], [2, None]])
            self.assertIsNone(cur.fetchone())
            self.assertTrue(cur.nextset())

            self.assertListOfListsEqual(cur.fetchall(), [])
            self.assertIsNone(cur.fetchone())
            self.assertFalse(cur.nextset())

    def test_bind_boolean(self):
        values = (True, 't', 'true', '1', 1, 'Yes', 'y', None,
                  False, 'f', 'false', '0', 0, 'No', 'n')
        expected = [[True] * 7 + [None] + [False] * 7]
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("""CREATE TABLE {} (
                    col_1 BOOL, col_2 BOOL, col_3 BOOL, col_4 BOOL, col_5 BOOL,
                    col_6 BOOL, col_7 BOOL, col_8 BOOL, col_9 BOOL, col_10 BOOL,
                    col_11 BOOL, col_12 BOOL, col_13 BOOL, col_14 BOOL, col_15 BOOL
                    )""".format(self._table))
            cur.execute("INSERT INTO {} VALUES ({})".format(
                        self._table, ','.join(['?']*15)), values)
            conn.commit()
            cur.execute("SELECT * FROM {}".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, expected)

    def test_bind_datetime(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE {} (a TIMESTAMP, b DATE, c TIME, d TIMETZ,"
                        "e TIMESTAMP, f DATE, g TIME, h TIMETZ)".format(self._table))
            values = [datetime(2018, 9, 7, 15, 38, 19, 769000), date(2018, 9, 7),
                      time(13, 50, 9), time(22, 36, 33, 123400, tzinfo=tzoffset(None, -19800)),
                      None, None, None, None]
            cur.execute("INSERT INTO {} VALUES ({})".format(self._table, ','.join(['?']*8)), values)
            conn.commit()
            cur.execute("SELECT * FROM {}".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [values])

    def test_bind_binary(self):
        values = [b'binary data', b'\\backslash data\\', u'\\backslash data\\',
                  u'\u00f1 encoding', 'raw data', 'long varbinary data', None]
        expected = [[b'binary data\x00\x00\x00', b'\\backslash data\\',
                     b'\\backslash data\\', b'\xc3\xb1 encoding',
                     b'raw data', b'long varbinary data', None]]
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("""CREATE TABLE {} (
                    col_1 BINARY(14), col_2 VARBINARY, col_3 VARBINARY,
                    col_4 BYTEA, col_5 RAW, col_6 LONG VARBINARY,
                    col_7 VARBINARY)""".format(self._table))
            cur.execute("INSERT INTO {} VALUES ({})".format(
                        self._table, ','.join(['?']*7)), values)
            conn.commit()
            cur.execute("SELECT * FROM {}".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, expected)

    def test_bind_numeric(self):
        values = [Decimal('123456789.98765'), Decimal('123456789.98765'),
                  Decimal('123456789.98765'), Decimal('123456789.98765'),
                  10, 11, 1234567890, 1296554905964, 123, 123, 123.45,
                  123.45, 123.45, 123.45, 123.45, None, None, None]
        expected = [[Decimal('123456789.9877'), Decimal('123456789.987650000000000'),
                     Decimal('123456790'), Decimal('123456789.987650000000000'),
                     10, 11, 1234567890, 1296554905964, 123, 123, 123.45,
                     123.45, 123.45, 123.45, 123.45, None, None, None]]
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("""CREATE TABLE {} (
                col_1 MONEY, col_2 NUMERIC, col_3 NUMBER, col_4 DECIMAL,
                col_5 TINYINT, col_6 SMALLINT, col_7 INT8, col_8 BIGINT,
                col_9 INT, col_10 INTEGER, col_11 DOUBLE PRECISION, col_12 REAL,
                col_13 FLOAT8, col_14 FLOAT(10), col_15 FLOAT,
                col_16 NUMERIC, col_17 INT, col_18 FLOAT
                )""".format(self._table))
            cur.execute("INSERT INTO {} VALUES ({})".format(
                        self._table, ','.join(['?']*18)), values)
            conn.commit()
            cur.execute("SELECT * FROM {}".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, expected)

    def test_bind_character(self):
        values = ['char data', b'raw varchar data',
                  u'long varbinary data \u00f1', None, None, None]
        expected = [['char data ', 'raw varchar data',
                     u'long varbinary data \u00f1', None, None, None]]
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("""CREATE TABLE {} (
                col_1 CHAR(10), col_2 VARCHAR, col_3 LONG VARCHAR,
                col_4 CHAR(10), col_5 VARCHAR, col_6 LONG VARCHAR
                )""".format(self._table))
            cur.execute("INSERT INTO {} VALUES ({})".format(
                        self._table, ','.join(['?']*6)), values)
            conn.commit()
            cur.execute("SELECT * FROM {}".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, expected)

    def test_bind_interval(self):
        values = ['1y 10m', '1 2', '1y 10m', '1y 10m',
                  '17910y 1h 3m 6s 5msecs 57us ago', '3 days 2 hours', '1 3',
                  '1y 15 mins 20 sec', '15 mins 20 sec', '1y 5 mins 20 sec',
                  '2 days 12 hours 15 mins 1235 milliseconds',
                  '2 days 12 hours 15 mins ago', '2 days 12 hours 15 mins ago',
                  None, None]
        expected = [[relativedelta(years=+1), relativedelta(years=+1, months=+2),
                     relativedelta(years=+1, months=+10), relativedelta(days=+365),
                     relativedelta(days=-6537150, hours=-1, minutes=-3, seconds=-6, microseconds=-5100),
                     relativedelta(days=+3, hours=+2), relativedelta(hours=+1, minutes=+3),
                     relativedelta(days=+365, minutes=+15, seconds=+20),
                     relativedelta(minutes=+15), relativedelta(days=+365, minutes=+5, seconds=+20),
                     relativedelta(days=+2, hours=+12, minutes=+15, seconds=+1, microseconds=+240000),
                     relativedelta(days=-2, hours=-12), relativedelta(days=-2, hours=-12, minutes=-15),
                     None, None]]
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("""CREATE TABLE {} (
                col_1 INTERVAL YEAR, col_2 INTERVAL YEAR TO MONTH,
                col_3 INTERVAL MONTH, col_4 INTERVAL DAY,
                col_5 INTERVAL DAY TO SECOND(4), col_6 INTERVAL HOUR,
                col_7 INTERVAL HOUR to MINUTE, col_8 INTERVAL HOUR to SECOND,
                col_9 INTERVAL MINUTE, col_10 INTERVAL MINUTE TO SECOND,
                col_11 INTERVAL SECOND(2), col_12 INTERVAL DAY TO HOUR,
                col_13 INTERVAL DAY TO MINUTE, col_14 INTERVAL YEAR TO MONTH,
                col_15 INTERVAL DAY TO SECOND
                )""".format(self._table))
            cur.execute("INSERT INTO {} VALUES ({})".format(
                        self._table, ','.join(['?']*15)), values)
            conn.commit()
            cur.execute("SELECT * FROM {}".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, expected)

    def test_bind_udtype(self):
        poly = "POLYGON ((1 2, 2 3, 3 1, 1 2))"
        line = "LINESTRING (42.1 71, 41.4 70, 41.3 72.9, 42.99 71.46, 44.47 73.21)"
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE {} (c1 GEOMETRY(10000), c2 GEOGRAPHY(1000))"
                .format(self._table))

            cur.execute("INSERT INTO {} VALUES (ST_GeomFromText(?), ST_GeographyFromText(?))"
                .format(self._table), [poly, line])
            conn.commit()
            cur.execute("SELECT c1, c2, ST_AsText(c1), ST_AsText(c2) FROM {}".format(self._table))

            res = cur.fetchall()
            self.assertEqual(res[0][2], poly)
            self.assertEqual(res[0][3], line)

            datatype_names = [col.type_name for col in cur.description]
            expected = ['geometry', 'geography', 'Long Varchar', 'Long Varchar']
            self.assertListEqual(datatype_names, expected)

            self.assertEqual(cur.description[0].display_size, 10000)
            self.assertEqual(cur.description[1].display_size, 1000)
