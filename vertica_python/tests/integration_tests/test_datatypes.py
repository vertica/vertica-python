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
