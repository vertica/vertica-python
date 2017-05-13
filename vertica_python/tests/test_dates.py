from __future__ import print_function, division, absolute_import

from collections import namedtuple
from datetime import date, datetime

from .base import VerticaPythonTestCase
from .. import errors
from ..vertica.column import timestamp_parse

DateTestingCase = namedtuple("DateTestingCase", ["string", "template", "date"])
TimestampTestingCase = namedtuple("TimestampTestingCase", ["string", "timestamp"])


class DateParsingTestCase(VerticaPythonTestCase):
    """Testing DATE type parsing with focus on 'AD'/'BC'.

    Note: the 'BC' or 'AD' era indicators in Vertica's date format seem to make Vertica behave as
    follows:
        1. Both 'BC' and 'AD' are simply a flags that tell Vertica: include era indicator if the
        date is Before Christ
        2. Dates in 'AD' will never include era indicator
    """

    def _test_dates(self, test_cases, msg=None):
        with self._connect() as conn:
            cur = conn.cursor()
            for tc in test_cases:
                cur.execute("SELECT TO_DATE('{0}', '{1}')".format(tc.string, tc.template))
                res = cur.fetchall()
                self.assertListOfListsEqual(res, [[tc.date]], msg=msg)

    def _test_not_supported(self, test_cases, msg=None):
        with self._connect() as conn:
            cur = conn.cursor()
            for tc in test_cases:
                with self.assertRaises(errors.NotSupportedError, msg=msg):
                    cur.execute("SELECT TO_DATE('{0}', '{1}')".format(tc.string, tc.template))
                    res = cur.fetchall()
                    self.assertListOfListsEqual(res, [[tc.date]])

    def test_no_to_no(self):
        test_cases = [
            DateTestingCase('1985-10-25', 'YYYY-MM-DD', date(1985, 10, 25)),
            DateTestingCase('1955-11-12', 'YYYY-MM-DD', date(1955, 11, 12)),
            DateTestingCase('1885-01-01', 'YYYY-MM-DD', date(1885, 1, 1)),
            DateTestingCase('2015-10-21', 'YYYY-MM-DD', date(2015, 10, 21)),
        ]
        self._test_dates(test_cases=test_cases, msg='no indicator -> no indicator')

    def test_ad_to_no(self):
        test_cases = [
            DateTestingCase('1985-10-25 AD', 'YYYY-MM-DD', date(1985, 10, 25)),
            DateTestingCase('1955-11-12 AD', 'YYYY-MM-DD', date(1955, 11, 12)),
            DateTestingCase('1885-01-01 AD', 'YYYY-MM-DD', date(1885, 1, 1)),
            DateTestingCase('2015-10-21 AD', 'YYYY-MM-DD', date(2015, 10, 21)),
        ]
        self._test_dates(test_cases=test_cases, msg='AD indicator -> no indicator')

    def test_bc_to_no(self):
        test_cases = [
            DateTestingCase('1985-10-25 BC', 'YYYY-MM-DD', date(1985, 10, 25)),
            DateTestingCase('1955-11-12 BC', 'YYYY-MM-DD', date(1955, 11, 12)),
            DateTestingCase('1885-01-01 BC', 'YYYY-MM-DD', date(1885, 1, 1)),
            DateTestingCase('2015-10-21 BC', 'YYYY-MM-DD', date(2015, 10, 21)),
        ]
        self._test_dates(test_cases=test_cases, msg='BC indicator -> no indicator')

    def test_no_to_ad(self):
        test_cases = [
            DateTestingCase('1985-10-25', 'YYYY-MM-DD AD', date(1985, 10, 25)),
            DateTestingCase('1955-11-12', 'YYYY-MM-DD AD', date(1955, 11, 12)),
            DateTestingCase('1885-01-01', 'YYYY-MM-DD AD', date(1885, 1, 1)),
            DateTestingCase('2015-10-21', 'YYYY-MM-DD AD', date(2015, 10, 21)),
        ]
        self._test_dates(test_cases=test_cases, msg='no indicator -> AD indicator')

    def test_ad_to_ad(self):
        test_cases = [
            DateTestingCase('1985-10-25 AD', 'YYYY-MM-DD AD', date(1985, 10, 25)),
            DateTestingCase('1955-11-12 AD', 'YYYY-MM-DD AD', date(1955, 11, 12)),
            DateTestingCase('1885-01-01 AD', 'YYYY-MM-DD AD', date(1885, 1, 1)),
            DateTestingCase('2015-10-21 AD', 'YYYY-MM-DD AD', date(2015, 10, 21)),
        ]
        self._test_dates(test_cases=test_cases, msg='AD indicator -> AD indicator')

    def test_bc_to_ad(self):
        test_cases = [
            DateTestingCase('1985-10-25 BC', 'YYYY-MM-DD AD', date(1985, 10, 25)),
            DateTestingCase('1955-11-12 BC', 'YYYY-MM-DD AD', date(1955, 11, 12)),
            DateTestingCase('1885-01-01 BC', 'YYYY-MM-DD AD', date(1885, 1, 1)),
            DateTestingCase('2015-10-21 BC', 'YYYY-MM-DD AD', date(2015, 10, 21)),
        ]
        self._test_not_supported(test_cases=test_cases, msg='BC indicator -> AD indicator')

    def test_no_to_bc(self):
        test_cases = [
            DateTestingCase('1985-10-25', 'YYYY-MM-DD BC', date(1985, 10, 25)),
            DateTestingCase('1955-11-12', 'YYYY-MM-DD BC', date(1955, 11, 12)),
            DateTestingCase('1885-01-01', 'YYYY-MM-DD BC', date(1885, 1, 1)),
            DateTestingCase('2015-10-21', 'YYYY-MM-DD BC', date(2015, 10, 21)),
        ]
        self._test_dates(test_cases=test_cases, msg='no indicator -> BC indicator')

    def test_ad_to_bc(self):
        test_cases = [
            DateTestingCase('1985-10-25 AD', 'YYYY-MM-DD BC', date(1985, 10, 25)),
            DateTestingCase('1955-11-12 AD', 'YYYY-MM-DD BC', date(1955, 11, 12)),
            DateTestingCase('1885-01-01 AD', 'YYYY-MM-DD BC', date(1885, 1, 1)),
            DateTestingCase('2015-10-21 AD', 'YYYY-MM-DD BC', date(2015, 10, 21)),
        ]
        self._test_dates(test_cases=test_cases, msg='AD indicator -> BC indicator')

    def test_bc_to_bc(self):
        test_cases = [
            DateTestingCase('1985-10-25 BC', 'YYYY-MM-DD BC', date(1985, 10, 25)),
            DateTestingCase('1955-11-12 BC', 'YYYY-MM-DD BC', date(1955, 11, 12)),
            DateTestingCase('1885-01-01 BC', 'YYYY-MM-DD BC', date(1885, 1, 1)),
            DateTestingCase('2015-10-21 BC', 'YYYY-MM-DD BC', date(2015, 10, 21)),
        ]
        self._test_not_supported(test_cases=test_cases, msg='BC indicator -> BC indicator')


class TimestampParsingTestCase(VerticaPythonTestCase):
    def _test_timestamps(self, test_cases, msg=None):
        for tc in test_cases:
            self.assertEqual(timestamp_parse(tc.string), tc.timestamp, msg=msg)

    def test_timestamp_second_resolution(self):
        test_cases = [  # back to the future dates
            TimestampTestingCase(
                '1985-10-26 01:25:01',
                datetime(year=1985, month=10, day=26, hour=1, minute=25, second=1)
            ),
            TimestampTestingCase(
                '1955-11-12 22:55:02',
                datetime(year=1955, month=11, day=12, hour=22, minute=55, second=2)
            ),
            TimestampTestingCase(
                '2015-10-21 11:12:03',
                datetime(year=2015, month=10, day=21, hour=11, minute=12, second=3)
            ),
            TimestampTestingCase(
                '1885-01-01 01:02:04',
                datetime(year=1885, month=1, day=1, hour=1, minute=2, second=4)
            ),
            TimestampTestingCase(
                '1885-09-02 02:03:05',
                datetime(year=1885, month=9, day=2, hour=2, minute=3, second=5)
            ),
        ]
        self._test_timestamps(test_cases=test_cases, msg='timestamp second resolution')

    def test_timestamp_microsecond_resolution(self):
        test_cases = [  # back to the future dates
            TimestampTestingCase(
                '1985-10-26 01:25:01.1',
                datetime(year=1985, month=10, day=26, hour=1, minute=25, second=1,
                         microsecond=100000)
            ),
            TimestampTestingCase(
                '1955-11-12 22:55:02.01',
                datetime(year=1955, month=11, day=12, hour=22, minute=55, second=2,
                         microsecond=10000)
            ),
            TimestampTestingCase(
                '2015-10-21 11:12:03.001',
                datetime(year=2015, month=10, day=21, hour=11, minute=12, second=3,
                         microsecond=1000)
            ),
            TimestampTestingCase(
                '1885-01-01 01:02:04.000001',
                datetime(year=1885, month=1, day=1, hour=1, minute=2, second=4,
                         microsecond=1)
            ),
            TimestampTestingCase(
                '1885-09-02 02:03:05.002343',
                datetime(year=1885, month=9, day=2, hour=2, minute=3, second=5,
                         microsecond=2343)
            ),
        ]
        self._test_timestamps(test_cases=test_cases, msg='timestamp microsecond resolution')

    def test_timestamp_year_over_9999_second_resolution(self):
        """Asserts that years over 9999 are truncated to 9999"""
        test_cases = [
            TimestampTestingCase(
                '19850-10-26 01:25:01',
                datetime(year=9999, month=10, day=26, hour=1, minute=25, second=1)
            ),
            TimestampTestingCase(
                '10000-11-12 22:55:02',
                datetime(year=9999, month=11, day=12, hour=22, minute=55, second=2)
            ),
            TimestampTestingCase(
                '9999-10-21 11:12:03',
                datetime(year=9999, month=10, day=21, hour=11, minute=12, second=3)
            ),
            TimestampTestingCase(
                '18850-01-01 01:02:04',
                datetime(year=9999, month=1, day=1, hour=1, minute=2, second=4)
            ),
            TimestampTestingCase(
                '18850-09-02 02:03:05',
                datetime(year=9999, month=9, day=2, hour=2, minute=3, second=5)
            ),
        ]
        self._test_timestamps(test_cases=test_cases, msg='timestamp past 9999 second resolution')

    def test_timestamp_year_over_9999_microsecond_resolution(self):
        test_cases = [
            TimestampTestingCase(
                '19850-10-26 01:25:01.1',
                datetime(year=9999, month=10, day=26, hour=1, minute=25, second=1,
                         microsecond=100000)
            ),
            TimestampTestingCase(
                '10000-11-12 22:55:02.01',
                datetime(year=9999, month=11, day=12, hour=22, minute=55, second=2,
                         microsecond=10000)
            ),
            TimestampTestingCase(
                '9999-10-21 11:12:03.001',
                datetime(year=9999, month=10, day=21, hour=11, minute=12, second=3,
                         microsecond=1000)
            ),
            TimestampTestingCase(
                '18850-01-01 01:02:04.000001',
                datetime(year=9999, month=1, day=1, hour=1, minute=2, second=4,
                         microsecond=1)
            ),
            TimestampTestingCase(
                '18850-09-02 02:03:05.002343',
                datetime(year=9999, month=9, day=2, hour=2, minute=3, second=5,
                         microsecond=2343)
            ),
        ]
        self._test_timestamps(test_cases=test_cases,
                              msg='timestamp past 9999 microsecond resolution')
