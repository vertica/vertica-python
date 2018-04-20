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

from .base import VerticaPythonTestCase

from .. import errors


class ErrorTestCase(VerticaPythonTestCase):
    def setUp(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS {0}".format(self._table))

    def test_missing_schema(self):
        with self._connect() as conn:
            cur = conn.cursor()
            with self.assertRaises(errors.MissingSchema):
                cur.execute("SELECT 1 FROM missing_schema.table")

    def test_missing_relation(self):
        with self._connect() as conn:
            cur = conn.cursor()
            with self.assertRaises(errors.MissingRelation):
                cur.execute("SELECT 1 FROM missing_table")

    def test_duplicate_object(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE {0} (a BOOLEAN)".format(self._table))
            with self.assertRaises(errors.DuplicateObject):
                cur.execute("CREATE TABLE {0} (a BOOLEAN)".format(self._table))
