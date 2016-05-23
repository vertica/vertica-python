from datetime import date, datetime
import pytz
from .test_commons import conn_info, VerticaTestCase
from .. import connect

class TimeZoneTestCase(VerticaTestCase):

    def test_simple_ts_query(self):
        query = """
        select
          to_timestamp('2016-05-15 13:15:17.789', 'YYYY-MM-DD HH:MI:SS.MS')
        ;
        """
        value = datetime(year=2016, month=5, day=15, hour=13, minute=15, second=17, microsecond=789000)

        with connect(**conn_info) as conn:
            cur = conn.cursor()
            cur.execute(query)
            res = cur.fetchall()
            assert res[0][0] == value

    def test_simple_ts_with_tz_query(self):
        query = """
        select
          to_timestamp_tz('2016-05-15 13:15:17.789 UTC', 'YYYY-MM-DD HH:MI:SS.MS TZ')
        ;
        """
        value = datetime(year=2016, month=5, day=15, hour=13, minute=15, second=17, microsecond=789000, tzinfo=pytz.utc)

        with connect(**conn_info) as conn:
            cur = conn.cursor()
            cur.execute(query)
            res = cur.fetchall()
            assert res[0][0] == value

    def test_simple_ts_with_offset_query(self):
        query = """
        select
          timestamp '2016-05-15 13:15:17.789+00'
        ;
        """
        value = datetime(year=2016, month=5, day=15, hour=13, minute=15, second=17, microsecond=789000, tzinfo=pytz.utc)

        with connect(**conn_info) as conn:
            cur = conn.cursor()
            cur.execute(query)
            res = cur.fetchall()
            assert res[0][0] == value
