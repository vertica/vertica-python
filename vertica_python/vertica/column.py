

from collections import namedtuple
import re

from builtins import str
from decimal import Decimal
from datetime import date
from datetime import datetime
from dateutil import parser
from vertica_python import errors

import pytz

years_re = re.compile(r'^([0-9]+)-')


# these methods are bad...
#
# a few timestamp with tz examples:
# 2013-01-01 00:00:00
# 2013-01-01 00:00:00+00
# 2013-01-01 00:00:00.01+00
# 2013-01-01 00:00:00.00001+00
#
# Vertica stores all data in UTC:
#   "TIMESTAMP WITH TIMEZONE (TIMESTAMPTZ) data is stored in GMT (UTC) by
#    converting data from the current local time zone to GMT."
# Vertica fetches data in local timezone:
#   "When TIMESTAMPTZ data is used, data is converted back to use the current
#    local time zone"
# If vertica boxes are on UTC, you should never have a non +00 offset (as
# far as I can tell) ie. inserting '2013-01-01 00:00:00.01 EST' to a
# timestamptz type stores: 2013-01-01 05:00:00.01+00
#       select t AT TIMEZONE 'America/New_York' returns: 2012-12-31 19:00:00.01
def timestamp_parse(s):
    s = str(s, 'utf-8')
    try:
        dt = _timestamp_parse(s)
    except ValueError:
        # Value error, year might be over 9999
        year_match = years_re.match(s)
        if year_match:
            year = year_match.groups()[0]
            dt = _timestamp_parse_without_year(s[len(year) + 1:])
            dt = dt.replace(year=int(year) % 10000)
        else:
            raise errors.DataError('Timestamp value not supported: %s' % s)

    return dt


def _timestamp_parse(s):
    if len(s) == 19:
        return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
    return datetime.strptime(s, '%Y-%m-%d %H:%M:%S.%f')


def _timestamp_parse_without_year(s):
    if len(s) == 14:
        return datetime.strptime(s, '%m-%d %H:%M:%S')
    return datetime.strptime(s, '%m-%d %H:%M:%S.%f')


def timestamp_tz_parse(s):
    s = str(s, 'utf-8')
    # if timezome is simply UTC...
    if s.endswith('+00'):
        # remove time zone
        ts = timestamp_parse(s[:-3].encode(encoding='utf-8', errors='strict'))
        ts = ts.replace(tzinfo=pytz.UTC)
        return ts
    # other wise do a real parse (slower)
    return parser.parse(s)


def date_parse(s):
    """
    Parses value of a DATE type.
    :param s: string to parse into date
    :return: an instance of datetime.date
    :raises NotSupportedError when a date Before Christ is encountered
    """
    if s.endswith(b' BC'):
        raise errors.NotSupportedError('Dates Before Christ are not supported. Got: ' + str(s, 'utf-8'))

    return date(*map(lambda x: int(x), s.split(b'-')))

ColumnTuple = namedtuple(
    'Column',
    ['name', 'type_code', 'display_size', 'internal_size',
     'precision', 'scale', 'null_ok']
)


class Column(object):

    @classmethod
    def data_type_conversions(cls, unicode_error=None):
        if unicode_error is None:
            unicode_error = 'strict'
        return [
            ('unspecified', None),
            ('tuple', None),
            ('pos', None),
            ('record', None),
            ('unknown', None),
            ('bool', lambda s: s == 't'),
            ('integer', lambda s: int(s)),
            ('float', lambda s: float(s)),
            ('char', lambda s: str(s, 'utf-8', unicode_error)),
            ('varchar', lambda s: str(s, 'utf-8', unicode_error)),
            ('date', date_parse),
            ('time', None),
            ('timestamp', timestamp_parse),
            ('timestamp_tz', timestamp_tz_parse),
            ('interval', None),
            ('time_tz', None),
            ('numeric', lambda s: Decimal(s)),
            ('bytea', None),
            ('rle_tuple', None),
        ]

    @property
    def data_types():
        return map(lambda x: x[0], Column.data_type_conversions())

    def __init__(self, col, unicode_error=None):
        self.name = col['name'].decode()
        self.type_code = col['data_type_oid']
        self.display_size = None
        self.internal_size = col['data_type_size']
        self.precision = None
        self.scale = None
        self.null_ok = None
        self.unicode_error = unicode_error
        self.data_type_conversions = Column.data_type_conversions(unicode_error=self.unicode_error)

        # WORKAROUND: Treat LONGVARCHAR as VARCHAR
        if self.type_code == 115:
            self.type_code = 9

        # Mark type_code as unspecified if not within known data types
        if self.type_code >= len(self.data_type_conversions):
            self.type_code = 0

        #self.props = ColumnTuple(col['name'], col['data_type_oid'], None, col['data_type_size'], None, None, None)
        self.props = ColumnTuple(self.name, self.type_code, None, col['data_type_size'], None, None, None)

        #self.converter = self.data_type_conversions[col['data_type_oid']][1]
        self.converter = self.data_type_conversions[self.type_code][1]

        # things that are actually sent
#        self.name = col['name']
#        self.data_type = self.data_type_conversions[col['data_type_oid']][0]
#        self.type_modifier = col['type_modifier']
#        self.format = 'text' if col['format_code'] == 0 else 'binary'
#        self.table_oid = col['table_oid']
#        self.attribute_number = col['attribute_number']
#        self.size = col['data_type_size']

    def convert(self, s):
        if s is None:
            return
        return self.converter(s) if self.converter is not None else s

    def __str__(self):
        return self.props.__str__()

    def __unicode__(self):
        return str(self.props.__str__())

    def __repr__(self):
        return self.props.__str__()

    def __iter__(self):
        for prop in self.props:
            yield prop

    def __getitem__(self, key):
        return self.props[key]
