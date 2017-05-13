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
