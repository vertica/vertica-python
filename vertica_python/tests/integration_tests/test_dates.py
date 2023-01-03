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
from datetime import date

from .base import VerticaPythonIntegrationTestCase
from ... import errors

DateTestingCase = namedtuple("DateTestingCase", ["string", "template", "date"])


class DateParsingTestCase(VerticaPythonIntegrationTestCase):
    """Testing DATE type parsing with focus on 'AD'/'BC'.

    Note: the 'BC' or 'AD' era indicators in Vertica's date format seem to make Vertica behave as
    follows:
        1. Both 'BC' and 'AD' are simply a flags that tell Vertica: include era indicator if the
        date is Before Christ
        2. Dates in 'AD' will never include era indicator
    """

    def _test_dates(self, test_cases, msg=None):
        with self._connect() as conn:
            cur = conn.cursor()
            for tc in test_cases:
                cur.execute("SELECT TO_DATE('{0}', '{1}')".format(tc.string, tc.template))
                res = cur.fetchall()
                self.assertListOfListsEqual(res, [[tc.date]], msg=msg)

    def _test_not_supported(self, test_cases, msg=None):
        with self._connect() as conn:
            cur = conn.cursor()
            for tc in test_cases:
                with self.assertRaises(errors.NotSupportedError, msg=msg):
                    cur.execute("SELECT TO_DATE('{0}', '{1}')".format(tc.string, tc.template))
                    res = cur.fetchall()
                    self.assertListOfListsEqual(res, [[tc.date]])

    def test_no_to_no(self):
        test_cases = [
            DateTestingCase('1985-10-25', 'YYYY-MM-DD', date(1985, 10, 25)),
            DateTestingCase('1955-11-12', 'YYYY-MM-DD', date(1955, 11, 12)),
            DateTestingCase('1885-01-01', 'YYYY-MM-DD', date(1885, 1, 1)),
            DateTestingCase('2015-10-21', 'YYYY-MM-DD', date(2015, 10, 21)),
        ]
        self._test_dates(test_cases=test_cases, msg='no indicator -> no indicator')

    def test_ad_to_no(self):
        test_cases = [
            DateTestingCase('1985-10-25 AD', 'YYYY-MM-DD', date(1985, 10, 25)),
            DateTestingCase('1955-11-12 AD', 'YYYY-MM-DD', date(1955, 11, 12)),
            DateTestingCase('1885-01-01 AD', 'YYYY-MM-DD', date(1885, 1, 1)),
            DateTestingCase('2015-10-21 AD', 'YYYY-MM-DD', date(2015, 10, 21)),
        ]
        self._test_dates(test_cases=test_cases, msg='AD indicator -> no indicator')

    def test_bc_to_no(self):
        test_cases = [
            DateTestingCase('1985-10-25 BC', 'YYYY-MM-DD', date(1985, 10, 25)),
            DateTestingCase('1955-11-12 BC', 'YYYY-MM-DD', date(1955, 11, 12)),
            DateTestingCase('1885-01-01 BC', 'YYYY-MM-DD', date(1885, 1, 1)),
            DateTestingCase('2015-10-21 BC', 'YYYY-MM-DD', date(2015, 10, 21)),
        ]
        self._test_dates(test_cases=test_cases, msg='BC indicator -> no indicator')

    def test_no_to_ad(self):
        test_cases = [
            DateTestingCase('1985-10-25', 'YYYY-MM-DD AD', date(1985, 10, 25)),
            DateTestingCase('1955-11-12', 'YYYY-MM-DD AD', date(1955, 11, 12)),
            DateTestingCase('1885-01-01', 'YYYY-MM-DD AD', date(1885, 1, 1)),
            DateTestingCase('2015-10-21', 'YYYY-MM-DD AD', date(2015, 10, 21)),
        ]
        self._test_dates(test_cases=test_cases, msg='no indicator -> AD indicator')

    def test_ad_to_ad(self):
        test_cases = [
            DateTestingCase('1985-10-25 AD', 'YYYY-MM-DD AD', date(1985, 10, 25)),
            DateTestingCase('1955-11-12 AD', 'YYYY-MM-DD AD', date(1955, 11, 12)),
            DateTestingCase('1885-01-01 AD', 'YYYY-MM-DD AD', date(1885, 1, 1)),
            DateTestingCase('2015-10-21 AD', 'YYYY-MM-DD AD', date(2015, 10, 21)),
        ]
        self._test_dates(test_cases=test_cases, msg='AD indicator -> AD indicator')

    def test_bc_to_ad(self):
        test_cases = [
            DateTestingCase('1985-10-25 BC', 'YYYY-MM-DD AD', date(1985, 10, 25)),
            DateTestingCase('1955-11-12 BC', 'YYYY-MM-DD AD', date(1955, 11, 12)),
            DateTestingCase('1885-01-01 BC', 'YYYY-MM-DD AD', date(1885, 1, 1)),
            DateTestingCase('2015-10-21 BC', 'YYYY-MM-DD AD', date(2015, 10, 21)),
        ]
        self._test_not_supported(test_cases=test_cases, msg='BC indicator -> AD indicator')

    def test_no_to_bc(self):
        test_cases = [
            DateTestingCase('1985-10-25', 'YYYY-MM-DD BC', date(1985, 10, 25)),
            DateTestingCase('1955-11-12', 'YYYY-MM-DD BC', date(1955, 11, 12)),
            DateTestingCase('1885-01-01', 'YYYY-MM-DD BC', date(1885, 1, 1)),
            DateTestingCase('2015-10-21', 'YYYY-MM-DD BC', date(2015, 10, 21)),
        ]
        self._test_dates(test_cases=test_cases, msg='no indicator -> BC indicator')

    def test_ad_to_bc(self):
        test_cases = [
            DateTestingCase('1985-10-25 AD', 'YYYY-MM-DD BC', date(1985, 10, 25)),
            DateTestingCase('1955-11-12 AD', 'YYYY-MM-DD BC', date(1955, 11, 12)),
            DateTestingCase('1885-01-01 AD', 'YYYY-MM-DD BC', date(1885, 1, 1)),
            DateTestingCase('2015-10-21 AD', 'YYYY-MM-DD BC', date(2015, 10, 21)),
        ]
        self._test_dates(test_cases=test_cases, msg='AD indicator -> BC indicator')

    def test_bc_to_bc(self):
        test_cases = [
            DateTestingCase('1985-10-25 BC', 'YYYY-MM-DD BC', date(1985, 10, 25)),
            DateTestingCase('1955-11-12 BC', 'YYYY-MM-DD BC', date(1955, 11, 12)),
            DateTestingCase('1885-01-01 BC', 'YYYY-MM-DD BC', date(1885, 1, 1)),
            DateTestingCase('2015-10-21 BC', 'YYYY-MM-DD BC', date(2015, 10, 21)),
        ]
        self._test_not_supported(test_cases=test_cases, msg='BC indicator -> BC indicator')


exec(DateParsingTestCase.createPrepStmtClass())
