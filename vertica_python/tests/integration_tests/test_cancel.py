# Copyright (c) 2019-2023 Micro Focus or one of its affiliates.
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

from multiprocessing import Process
import pytest
import time

from .base import VerticaPythonIntegrationTestCase
from ... import errors


class CancelTestCase(VerticaPythonIntegrationTestCase):

    def test_cursor_cancel(self):
        # Cursor.cancel() should be not supported any more
        with self._connect() as conn:
            cursor = conn.cursor()
            with self.assertRaises(errors.NotSupportedError):
                cursor.cancel()

    def test_connection_cancel_no_query(self):
        with self._connect() as conn:
            cur = conn.cursor()
            # No query is being executed, cancel does nothing
            conn.cancel()

    @pytest.mark.timeout(30)
    def test_connection_cancel_running_query(self):
        def cancel_query(conn, delay=5):
            time.sleep(delay)
            conn.cancel()

        with self._connect() as conn:
            cur = conn.cursor()
            p1 = Process(target=cancel_query, args=(conn,))
            p1.start()
            with self.assertRaises(errors.QueryCanceled):
                long_running_query = ('select count(*) from '
                    '(select node_name from CONFIGURATION_PARAMETERS) as a cross join '
                    '(select node_name from CONFIGURATION_PARAMETERS) as b cross join '
                    '(select node_name from CONFIGURATION_PARAMETERS) as c')
                cur.execute(long_running_query)
            p1.join()

            # Must be able to successfully run next query
            cur.execute("SELECT 1")
            res = cur.fetchall()
            self.assertListOfListsEqual(res, [[1]])

    def test_connection_cancel_returned_query(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS vptest")
            try:
                # Creating and loading table
                cur.execute("CREATE TABLE vptest(id INTEGER, time TIMESTAMP)")
                cur.execute("INSERT INTO vptest"
                        " SELECT row_number() OVER(), slice_time"
                        "    FROM("
                        "         SELECT slice_time FROM("
                        "         SELECT '2021-01-01'::timestamp s UNION ALL SELECT '2022-01-01'::timestamp s"
                        "          ) sq TIMESERIES slice_time AS '1 second' OVER(ORDER BY s)"
                        "    ) sq2")

                # This query returns over 30,000,000 rows. We cancel the command after
                # reading 100 of them, and then continue reading results. This quickly
                # results in an exception being thrown due to the cancel having taken effect.
                cur.execute("SELECT id, time FROM vptest")

                nCount = 0
                with self.assertRaises(errors.QueryCanceled):
                    while cur.fetchone():
                        nCount += 1
                        if nCount == 100:
                            conn.cancel()

                # The number of rows read after the cancel message is sent to the server can vary.
                # 1,000,000 seems to leave a safe margin while still falling well short of
                # the 30,000,000+ rows we'd have read if the cancel didn't work.
                self.assertTrue(100 <= nCount < 1000000)

                # Must be able to successfully run next query
                cur.execute("SELECT 1")
                res = cur.fetchall()
                self.assertListOfListsEqual(res, [[1]])
            finally:
                cur.execute("DROP TABLE IF EXISTS vptest")


exec(CancelTestCase.createPrepStmtClass())
