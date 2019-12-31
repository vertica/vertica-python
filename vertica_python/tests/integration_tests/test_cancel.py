# Copyright (c) 2019-2020 Micro Focus or one of its affiliates.
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


exec(CancelTestCase.createPrepStmtClass())
