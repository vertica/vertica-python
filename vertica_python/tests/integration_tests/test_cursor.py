# Copyright (c) 2018 Micro Focus or one of its affiliates.
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

import datetime
import logging
import os as _os
import re
import tempfile

from .base import VerticaPythonIntegrationTestCase
from ... import errors


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

    def test_inline_commit(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO {0} (a, b) VALUES (1, 'aa'); COMMIT;".format(self._table))
            cur.execute("SELECT a, b FROM {0} WHERE a = 1".format(self._table))

            # unknown rowcount
            self.assertEqual(cur.rowcount, -1)

            res = cur.fetchall()
            self.assertEqual(cur.rowcount, 1)

            self.assertListOfListsEqual(res, [[1, 'aa']])

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

    def test_copy_with_file(self):
        with tempfile.TemporaryFile() as tmpfile, self._connect() as conn1, self._connect() as conn2:
            if _os.name != 'posix' or _os.sys.platform == 'cygwin':
                f = getattr(tmpfile, 'file')
            else:
                f = tmpfile

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
            except errors.Error as e:
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
            except errors.Error as e:
                cur.execute("SELECT 1;")
                res = cur.fetchall()

            self.assertListOfListsEqual(res, [[1]])

    def test_with_conn(self):
        with self._connect() as conn:
            cur = conn.cursor()

            cur.execute("INSERT INTO {0} (a, b) VALUES (1, 'aa'); COMMIT;".format(self._table))
            cur.execute("SELECT a, b FROM {0} WHERE a = 1".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [[1, 'aa']])

    def test_iterator(self):
        with self._connect() as conn:
            cur = conn.cursor()
            values = [[1, 'aa'], [2, 'bb'], [3, 'cc']]

            for n, s in values:
                cur.execute("INSERT INTO {0} (a, b) VALUES (:n, :s)".format(self._table),
                            {'n': n, 's': s})
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
                cur.execute("INSERT INTO {0} (a, b) VALUES (:n, :s)".format(self._table),
                            {'n': n, 's': s})
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
            cur.execute("INSERT INTO {0} (a, b) VALUES (1, 'aa'); COMMIT;".format(self._table))
            with self.assertRaises(errors.QueryError):
                cur.execute("SELECT * FROM {0}_fail".format(self._table))

            # verify cursor still usable after errors
            cur.execute("SELECT a, b FROM {0} WHERE a = 1".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [[1, 'aa']])

    def test_cursor_close_and_reuse(self):
        with self._connect() as conn:
            cur = conn.cursor()

            # insert data
            cur.execute("INSERT INTO {0} (a, b) VALUES (2, 'bb'); COMMIT;".format(self._table))

            # (query -> close -> reopen) * 3 times
            for _ in range(3):
                cur.execute("SELECT a, b FROM {0} WHERE a = 2".format(self._table))
                res = cur.fetchall()
                self.assertListOfListsEqual(res, [[2, 'bb']])

                # close and reopen cursor
                cur.close()
                cur = conn.cursor()

    # unit test for #74
    def test_nextset(self):
        with self._connect() as conn:
            cur = conn.cursor()

            cur.execute("SELECT 1; SELECT 2;")

            res1 = cur.fetchall()
            self.assertListOfListsEqual(res1, [[1]])
            self.assertIsNone(cur.fetchone())
            self.assertTrue(cur.nextset())

            res2 = cur.fetchall()
            self.assertListOfListsEqual(res2, [[2]])
            self.assertIsNone(cur.fetchone())
            self.assertFalse(cur.nextset())

    # unit test for #74
    def test_nextset_with_delete(self):
        with self._connect() as conn:
            cur = conn.cursor()

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

    # unit test for #144
    def test_empty_query(self):
        with self._connect() as conn:
            cur = conn.cursor()

            cur.execute("")
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [])

    def test_format_quote_unicode(self):
        with self._connect() as conn:
            cur = conn.cursor()
            bad_word = u'Fr\xfchst\xfcck'
            formatted_word = u''.join((u'"', re.escape(bad_word), u'"'))
            self.assertEqual(formatted_word, cur.format_quote(bad_word, True))
            
    # unit test for #175
    def test_datetime_types(self):
        with self._connect() as conn:
            cur = conn.cursor()

            # clean old table
            cur.execute("DROP TABLE IF EXISTS {0}".format(self._table))

            # create test table
            cur.execute("""CREATE TABLE {0} (
                                a INT,
                                b VARCHAR(32),
                                c TIMESTAMP,
                                d DATE,
                                e TIME
                           )
                        """.format(self._table))

            cur.execute("INSERT INTO {0} (a, b, c, d, e) VALUES (:n, :s, :dt, :d, :t)".format(self._table),
                            {'n': 10, 's': 'aa',
                             'dt': datetime.datetime(2018, 9, 7, 15, 38, 19, 769000),
                             'd': datetime.date(2018, 9, 7),
                             't': datetime.time(13, 50, 9)})
            conn.commit()

            cur.execute("SELECT a, b, c, d, e FROM {0}".format(self._table))
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [[10, 'aa', datetime.datetime(2018, 9, 7, 15, 38, 19, 769000),
                                               datetime.date(2018, 9, 7), datetime.time(13, 50, 9)]])

            # clean up
            cur.execute("DROP TABLE IF EXISTS {0}".format(self._table))


class ExecutemanyTestCase(VerticaPythonIntegrationTestCase):
    def setUp(self):
        super(ExecutemanyTestCase, self).setUp()
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
        super(ExecutemanyTestCase, self).tearDown()

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
        self._test_executemany(self._table, [(1, 'aa'), (2, 'bb')])

    def test_executemany_quoted_path(self):
        table = '.'.join(['"{}"'.format(s.strip('"')) for s in self._table.split('.')])
        self._test_executemany(table, [(1, 'aa'), (2, 'bb')])

    def test_executemany_utf8(self):
        self._test_executemany(self._table, [(1, u'a\xfc'), (2, u'bb')])

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
