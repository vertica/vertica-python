from .test_commons import conn_info, VerticaTestCase
from .. import connect


class ColumnTestCase(VerticaTestCase):
    def test_column_names_query(self):
        column_0 = 'isocode'
        column_1 = 'name'
        query = """
        select 'US' as {column_0}, 'United States' as {column_1}
        union all
        select 'CA', 'Canada'
        union all
        select 'MX', 'Mexico'
        """.format(column_0=column_0, column_1=column_1)
        with connect(**conn_info) as conn:
            cur = conn.cursor()
            cur.execute(query)
            description = cur.description
            assert description[0].name == column_0
            assert description[1].name == column_1
