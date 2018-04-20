# Copyright (c) 2013-2017 Uber Technologies, Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function, division, absolute_import

from decimal import Decimal

from .base import VerticaPythonTestCase


class TypeTestCase(VerticaPythonTestCase):
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
