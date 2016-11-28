from decimal import Decimal

from .test_commons import conn_info, VerticaTestCase
from .. import connect


class TypeTestCase(VerticaTestCase):
    @staticmethod
    def _execute_and_fetchone(query):
        with connect(**conn_info) as conn:
            cur = conn.cursor()
            cur.execute(query)
            return cur.fetchone()

    def test_decimal_query(self):
        value = Decimal(0.42)
        query = "SELECT {}::numeric".format(value)
        res = self._execute_and_fetchone(query)
        self.assertAlmostEqual(res[0], value)

    def test_boolean_query__true(self):
        value = True
        query = "SELECT {}::boolean".format(value)
        res = self._execute_and_fetchone(query)
        self.assertEqual(res[0], value)

    def test_boolean_query__false(self):
        value = False
        query = "SELECT {}::boolean".format(value)
        res = self._execute_and_fetchone(query)
        self.assertEqual(res[0], value)
