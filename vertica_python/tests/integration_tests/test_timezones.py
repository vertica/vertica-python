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

from collections import namedtuple
from datetime import datetime
from dateutil import tz

from .base import VerticaPythonIntegrationTestCase

TimeZoneTestingCase = namedtuple("TimeZoneTestingCase", ["string", "template", "timestamp"])


class TimeZoneTestCase(VerticaPythonIntegrationTestCase):
    def _test_ts(self, test_cases):
        with self._connect() as conn:
            cur = conn.cursor()
            for tc in test_cases:
                cur.execute("SELECT TO_TIMESTAMP('{0}', '{1}')".format(tc.string, tc.template))
                res = cur.fetchone()
                self.assertEqual(tc.timestamp.toordinal(), res[0].toordinal())

    def test_simple_ts_query(self):
        template = 'YYYY-MM-DD HH:MI:SS.MS'
        test_cases = [
            TimeZoneTestingCase(
                string='2016-05-15 13:15:17.789', template=template,
                timestamp=datetime(year=2016, month=5, day=15, hour=13, minute=15, second=17,
                                   microsecond=789000)
            ),
        ]
        self._test_ts(test_cases=test_cases)

    def test_simple_ts_with_tz_query(self):
        template = 'YYYY-MM-DD HH:MI:SS.MS TZ'
        test_cases = [
            TimeZoneTestingCase(
                string='2016-05-15 13:15:17.789 UTC', template=template,
                timestamp=datetime(year=2016, month=5, day=15, hour=13, minute=15, second=17,
                                   microsecond=789000, tzinfo=tz.tzutc())
            ),
        ]
        self._test_ts(test_cases=test_cases)

    def test_simple_ts_with_offset_query(self):
        template = 'YYYY-MM-DD HH:MI:SS.MS+00'
        test_cases = [
            TimeZoneTestingCase(
                string='2016-05-15 13:15:17.789 UTC', template=template,
                timestamp=datetime(year=2016, month=5, day=15, hour=13, minute=15, second=17,
                                   microsecond=789000, tzinfo=tz.tzutc())
            ),
        ]
        self._test_ts(test_cases=test_cases)


exec(TimeZoneTestCase.createPrepStmtClass())
