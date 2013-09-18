from __future__ import absolute_import

from decimal import Decimal
from datetime import date
from datetime import datetime
from dateutil import parser
import pytz


# these methods are bad...
#
# a few timestamp with tz examples:
# 2013-01-01 00:00:00
# 2013-01-01 00:00:00+00
# 2013-01-01 00:00:00.01+00
# 2013-01-01 00:00:00.00001+00
#
# Vertica stores all data in UTC: 
#   "TIMESTAMP WITH TIMEZONE (TIMESTAMPTZ) data is stored in GMT (UTC) by converting data from the current local time zone to GMT."
# Vertica fetches data in local timezone:
#   "When TIMESTAMPTZ data is used, data is converted back to use the current local time zone"
# If vertica boxes are on UTC, you should never have a non +00 offset (as far as i can tell)
#   ie. inserting '2013-01-01 00:00:00.01 EST' to a timestamptz type stores: 2013-01-01 05:00:00.01+00
#       select t AT TIMEZONE 'America/New_York' returns: 2012-12-31 19:00:00.01
def timestamp_parse(s):
    if len(s) == 19:
        return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
    return datetime.strptime(s, '%Y-%m-%d %H:%M:%S.%f')

def timestamp_tz_parse(s):
    # if timezome is simply UTC...
    if s.endswith('+00'):
        # remove time zone
        ts = timestamp_parse(s[:-3])
        ts = ts.replace(tzinfo=pytz.UTC)
        return ts
    # other wise do a real parse (slower)
    return parser.parse(s)

class Column(object):

    DATA_TYPE_CONVERSIONS = [
        ['unspecified', None],
        ['tuple', None],
        ['pos', None],
        ['record', None],
        ['unknown', None],
        ['bool', lambda s: s == 't'],
        ['integer', lambda s: int(s)],
        ['float', lambda s: float(s)],
        ['char', lambda s: unicode(s, 'utf-8')],
        ['varchar', lambda s: unicode(s, 'utf-8')],
        ['date', lambda s: date(*map(lambda x: int(x), s.split('-')))],
        ['time', None],
        ['timestamp', timestamp_parse],
        ['timestamp_tz', timestamp_tz_parse],
        ['interval', None],
        ['time_tz', None],
        ['numeric', lambda s: Decimal(s)],
        ['bytea', None],
        ['rle_tuple', None],
    ]

    DATA_TYPES = map(lambda x: x[0], DATA_TYPE_CONVERSIONS)

    def __init__(self, col):
        self.type_modifier = col['type_modifier']
        self.format = 'text' if col['format_code'] == 0 else 'binary'
        self.table_oid = col['table_oid']
        self.name = col['name']
        self.attribute_number = col['attribute_number']
        self.data_type = self.DATA_TYPE_CONVERSIONS[col['data_type_oid']][0]
        self.converter = self.DATA_TYPE_CONVERSIONS[col['data_type_oid']][1]
        self.size = col['data_type_size']

    def convert(self, s):
        if s is None:
            return
        return self.converter(s) if self.converter is not None else s
