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


class ColumnTestCase(VerticaPythonTestCase):
    def test_column_names_query(self):
        columns = ['isocode', 'name']

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT 'US' AS {0}, 'United States' AS {1}
                UNION ALL SELECT 'CA', 'Canada'
                UNION ALL SELECT 'MX', 'Mexico' """.format(*columns))
            description = cur.description

        self.assertListEqual([d.name for d in description], columns)
