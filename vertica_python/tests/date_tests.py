from datetime import date
from test_commons import *
from vertica_python import errors


class DateParsingTestCase(VerticaTestCase):
    """
    Testing DATE type parsing with focus on AD/BC and lack of support for dates Before Christ.

    Note: the 'BC' or 'AD' era indicators in Vertica's date format seem to make Vertica behave as follows:

     - Both 'BC' and 'AD' are simply a flags that tell Vertica: include era indicator if the date is Before
       Christ
     - Dates in AD will never include era indicator
    """
    def _query_to_date(self, expression, pattern):
        return self.query_and_fetchall("SELECT TO_DATE('%(expression)s', '%(pattern)s')" % locals())

    def _assert_date(self, expression, pattern, expected):
        res = self._query_to_date(expression, pattern)

        if len(res) == 0:
            self.fail("Expected that query '%(query)s' would return one row with one column. Got nothing." % locals())

        elif len(res[0]) == 0:
            self.fail("Expected that query '%(query)s' would return one row and one column. Got one row and no column."
                      % locals())

        self.assertEqual(expected, res[0][0], "Expected date '%s' but got: '%s'" % (str(expected), str(res[0][0])))

    def test_after_christ(self):
        self._assert_date('2000-01-01 AD', 'YYYY-MM-DD BC', date(2000, 1, 1))
        self._assert_date('2000-01-01 AD', 'YYYY-MM-DD AD', date(2000, 1, 1))
        self._assert_date('2000-01-01', 'YYYY-MM-DD', date(2000, 1, 1))

    def test_before_christ_bc_indicator(self):
        try:
            res = self._query_to_date('2000-01-01 BC', 'YYYY-MM-DD BC')

            self.fail("Expected to see NotSupportedError when Before Christ date is encountered. Got: " + str(res))
        except errors.NotSupportedError:
            pass

    def test_before_christ_ad_indicator(self):
        try:
            res = self._query_to_date('2000-01-01 BC', 'YYYY-MM-DD AD')

            self.fail("Expected to see NotSupportedError when Before Christ date is encountered. Got: " + str(res))
        except errors.NotSupportedError:
            pass
