from .test_commons import conn_info, VerticaTestCase
from .. import connect
from decimal import Decimal


class TypeTestCase(VerticaTestCase):
    def test_decimal_query(self):
        value = Decimal(0.42)
        query = "SELECT {}::numeric".format(value)

        with connect(**conn_info) as conn:
            cur = conn.cursor()
            cur.execute(query)
            res = cur.fetchone()
            self.assertAlmostEqual(res[0], value)
