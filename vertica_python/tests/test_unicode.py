from __future__ import print_function, division, absolute_import

from .base import VerticaPythonTestCase


class UnicodeTestCase(VerticaPythonTestCase):
    def test_unicode_query(self):
        value = u'\u16a0'
        query = u"SELECT '{0}'".format(value)

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query)
            res = cur.fetchone()

        self.assertResultEqual(value, res[0])

    def test_unicode_list_parameter(self):
        values = [u'\u00f1', 'foo', 3]
        query = u"SELECT {0}".format(", ".join(["%s"] * len(values)))

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query, tuple(values))
            results = cur.fetchone()

        for val, res in zip(values, results):
            self.assertResultEqual(val, res)

    def test_unicode_named_parameter_binding(self):
        values = [u'\u16b1', 'foo', 3]
        keys = [u'\u16a0', 'foo', 3]

        query = u"SELECT {0}".format(", ".join([u":{0}".format(key) for key in keys]))

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query, dict(zip(keys, values)))
            results = cur.fetchone()

        for val, res in zip(values, results):
            self.assertResultEqual(val, res)

    def test_string_query(self):
        value = u'test'
        query = u"SELECT '{0}'".format(value)

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query)
            res = cur.fetchone()

        self.assertEqual(value, res[0])

    def test_string_named_parameter_binding(self):
        key = u'test'
        value = u'value'
        query = u"SELECT :{0}".format(key)

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query, {key: value})
            res = cur.fetchone()

        self.assertResultEqual(value, res[0])

    # unit test for issue #160
    def test_null_named_parameter_binding(self):
        key = u'test'
        value = None
        query = u"SELECT :{0}".format(key)

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query, {key: value})
            res = cur.fetchone()

        self.assertResultEqual(value, res[0])

    # unit test for issue #160
    def test_null_list_parameter(self):
        values = [u'\u00f1', 'foo', None]
        query = u"SELECT {0}".format(", ".join(["%s"] * len(values)))

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query, tuple(values))
            results = cur.fetchone()

        for val, res in zip(values, results):
            self.assertResultEqual(val, res)
