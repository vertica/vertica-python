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
    def test_unicode_list_parameter(self):
        v1 = u'\u00f1'
        v2 = 'foo'
        v3 = 3
        query = u"SELECT %s, %s, %s"

        with connect(**conn_info) as conn:
            cur = conn.cursor()
            cur.execute(query, (v1, v2, v3))
            res = cur.fetchone()

            assert res[0] == v1
            assert res[1] == v2
            assert res[2] == v3

    # this test is broken on python3: see issue #112
    def test_unicode_named_parameter_binding(self):
        k1 = u'\u16a0'
        k2 = 'foo'
        k3 = 3

        v1 = u'\u16b1'
        v2 = 'foo'
        v3 = 3

        query = u"SELECT :{}, :{}, :{}".format(k1, k2, k3)

        with connect(**conn_info) as conn:
            cur = conn.cursor()
            cur.execute(query, {k1: v1, k2: v2, k3: v3})
            res = cur.fetchone()

            assert res[0] == v1
            assert res[1] == v2
            assert res[2] == v3

    def test_string_query(self):
        value = u'test'
        query = u"SELECT '{}'".format(value)

        with connect(**conn_info) as conn:
            cur = conn.cursor()
            cur.execute(query)
            res = cur.fetchone()

            assert res[0] == value

    def test_string_named_parameter_binding(self):
        key = u'test'
        value = u'value'
        query = u"SELECT :{}".format(key)

        with connect(**conn_info) as conn:
            cur = conn.cursor()
            cur.execute(query, {key: value})
            res = cur.fetchone()

            assert res[0] == value
