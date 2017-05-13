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
