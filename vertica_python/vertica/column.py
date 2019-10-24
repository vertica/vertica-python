# Copyright (c) 2018-2019 Micro Focus or one of its affiliates.
# Copyright (c) 2018 Uber Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Copyright (c) 2013-2017 Uber Technologies, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.


from __future__ import print_function, division, absolute_import

import re
from collections import namedtuple
from datetime import date, datetime
from decimal import Decimal

# noinspection PyCompatibility,PyUnresolvedReferences
from builtins import str
from dateutil import parser, tz

from .. import errors
from .. import datatypes
from ..compat import as_str, as_text


YEARS_RE = re.compile(r"^([0-9]+)-")


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
    s = as_str(s)
    try:
        dt = _timestamp_parse(s)
    except ValueError:
        # Value error, year might be over 9999
        year_match = YEARS_RE.match(s)
        if year_match:
            year = year_match.groups()[0]
            dt = _timestamp_parse_without_year(s[len(year) + 1:])
            dt = dt.replace(year=min(int(year), 9999))
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
    s = as_str(s)
    # if timezone is simply UTC...
    if s.endswith('+00'):
        # remove time zone
        ts = timestamp_parse(s[:-3].encode(encoding='utf-8', errors='strict'))
        ts = ts.replace(tzinfo=tz.tzutc())
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
    s = as_str(s)
    if s.endswith(' BC'):
        raise errors.NotSupportedError('Dates Before Christ are not supported. Got: {0}'.format(s))

    # Value error, year might be over 9999
    return date(*map(lambda x: min(int(x), 9999), s.split('-')))


def time_parse(s):
    s = as_str(s)
    if len(s) == 8:
        return datetime.strptime(s, '%H:%M:%S').time()
    return datetime.strptime(s, '%H:%M:%S.%f').time()


ColumnTuple = namedtuple('Column', ['name', 'type_code', 'display_size', 'internal_size',
                                    'precision', 'scale', 'null_ok'])


class Column(object):
    def __init__(self, col, unicode_error=None):
        self.name = col['name']
        self.type_code = col['data_type_oid']
        self.type_name = col['data_type_name']
        self.display_size = datatypes.getDisplaySize(col['data_type_oid'], col['type_modifier'])
        self.internal_size = col['data_type_size']
        self.precision = datatypes.getPrecision(col['data_type_oid'], col['type_modifier'])
        self.scale = datatypes.getScale(col['data_type_oid'], col['type_modifier'])
        self.null_ok = col['null_ok']
        self.is_identity = col['is_identity']
        self.unicode_error = unicode_error
        self.data_type_conversions = Column._data_type_conversions(unicode_error=self.unicode_error)

        self.props = ColumnTuple(self.name, self.type_code, self.display_size, self.internal_size,
                                 self.precision, self.scale, self.null_ok)

        # WORKAROUND: Treat LONGVARCHAR as VARCHAR
        if self.type_code == 115:
            self.type_code = 9

        # Mark type_code as unspecified if not within known data types
        if self.type_code >= len(self.data_type_conversions):
            self.type_code = 0

        # self.converter = self.data_type_conversions[col['data_type_oid']][1]
        self.converter = self.data_type_conversions[self.type_code][1]

        # things that are actually sent
        # self.name = col['name']
        # self.data_type = self.data_type_conversions[col['data_type_oid']][0]
        # self.type_modifier = col['type_modifier']
        # self.format = 'text' if col['format_code'] == 0 else 'binary'
        # self.table_oid = col['table_oid']
        # self.attribute_number = col['attribute_number']
        # self.size = col['data_type_size']

    @classmethod
    def _data_type_conversions(cls, unicode_error=None):
        if unicode_error is None:
            unicode_error = 'strict'
        return [
            ('unspecified', None),
            ('tuple', None),
            ('pos', None),
            ('record', None),
            ('unknown', None),
            ('bool', lambda s: 't' == str(s, encoding='utf-8', errors=unicode_error)),
            ('integer', lambda s: int(s)),
            ('float', lambda s: float(s)),
            ('char', lambda s: str(s, encoding='utf-8', errors=unicode_error)),
            ('varchar', lambda s: str(s, encoding='utf-8', errors=unicode_error)),
            ('date', date_parse),
            ('time', time_parse),
            ('timestamp', timestamp_parse),
            ('timestamp_tz', timestamp_tz_parse),
            ('interval', None),
            ('time_tz', None),
            ('numeric',
             lambda s: Decimal(str(s, encoding='utf-8', errors=unicode_error))),
            ('bytea', None),
            ('rle_tuple', None),
        ]

    @classmethod
    def data_types(cls):
        return tuple([name for name, value in cls._data_type_conversions()])

    def convert(self, s):
        if s is None:
            return
        return self.converter(s) if self.converter is not None else s

    def __str__(self):
        return as_str(str(self.props))

    def __unicode__(self):
        return as_text(str(self.props))

    def __repr__(self):
        return as_str(str(self.props))

    def __iter__(self):
        for prop in self.props:
            yield prop

    def __getitem__(self, key):
        return self.props[key]
