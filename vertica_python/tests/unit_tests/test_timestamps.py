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

from .base import VerticaPythonUnitTestCase
from ...vertica.deserializer import load_timestamp_text

TimestampTestingCase = namedtuple("TimestampTestingCase", ["string", "timestamp"])


class TimestampParsingTestCase(VerticaPythonUnitTestCase):
    def _test_timestamps(self, test_cases, msg=None):
        for tc in test_cases:
            self.assertEqual(load_timestamp_text(tc.string, None), tc.timestamp, msg=msg)

    def test_timestamp_second_resolution(self):
        test_cases = [  # back to the future dates
            TimestampTestingCase(
                '1985-10-26 01:25:01',
                datetime(year=1985, month=10, day=26, hour=1, minute=25, second=1)
            ),
            TimestampTestingCase(
                '1955-11-12 22:55:02',
                datetime(year=1955, month=11, day=12, hour=22, minute=55, second=2)
            ),
            TimestampTestingCase(
                '2015-10-21 11:12:03',
                datetime(year=2015, month=10, day=21, hour=11, minute=12, second=3)
            ),
            TimestampTestingCase(
                '1885-01-01 01:02:04',
                datetime(year=1885, month=1, day=1, hour=1, minute=2, second=4)
            ),
            TimestampTestingCase(
                '1885-09-02 02:03:05',
                datetime(year=1885, month=9, day=2, hour=2, minute=3, second=5)
            ),
        ]
        self._test_timestamps(test_cases=test_cases, msg='timestamp second resolution')

    def test_timestamp_microsecond_resolution(self):
        test_cases = [  # back to the future dates
            TimestampTestingCase(
                '1985-10-26 01:25:01.1',
                datetime(year=1985, month=10, day=26, hour=1, minute=25, second=1,
                         microsecond=100000)
            ),
            TimestampTestingCase(
                '1955-11-12 22:55:02.01',
                datetime(year=1955, month=11, day=12, hour=22, minute=55, second=2,
                         microsecond=10000)
            ),
            TimestampTestingCase(
                '2015-10-21 11:12:03.001',
                datetime(year=2015, month=10, day=21, hour=11, minute=12, second=3,
                         microsecond=1000)
            ),
            TimestampTestingCase(
                '1885-01-01 01:02:04.000001',
                datetime(year=1885, month=1, day=1, hour=1, minute=2, second=4,
                         microsecond=1)
            ),
            TimestampTestingCase(
                '1885-09-02 02:03:05.002343',
                datetime(year=1885, month=9, day=2, hour=2, minute=3, second=5,
                         microsecond=2343)
            ),
        ]
        self._test_timestamps(test_cases=test_cases, msg='timestamp microsecond resolution')

