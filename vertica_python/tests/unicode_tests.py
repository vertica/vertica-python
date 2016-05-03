from .test_commons import conn_info, VerticaTestCase
from .. import connect


class UnicodeTestCase(VerticaTestCase):
    def test_unicode_query(self):
        value = u'\u16a0'
        query = u"SELECT '{}'".format(value)

        with connect(**conn_info) as conn:
            cur = conn.cursor()
            cur.execute(query)
            res = cur.fetchone()

            assert res[0] == value

    # this test is broken on python3: see issue #112
    def test_unicode_named_parameter_binding(self):
        key = u'\u16a0'
        value = u'\u16b1'
        query = u"SELECT :{}".format(key)

        with connect(**conn_info) as conn:
            cur = conn.cursor()
            cur.execute(query, {key: value})
            res = cur.fetchone()

            assert res[0] == value
