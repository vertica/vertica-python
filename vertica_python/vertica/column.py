# Copyright (c) 2018-2022 Micro Focus or one of its affiliates.
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
from six import PY2
from uuid import UUID

# noinspection PyCompatibility,PyUnresolvedReferences
from dateutil import parser, tz

from .. import errors
from ..datatypes import VerticaType, getDisplaySize, getPrecision, getScale
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

def binary_data_parse(s):
    """
    Parses text value of a BINARY/VARBINARY/LONG VARBINARY type.
    :param s: bytearray
    :return: bytes
    """
    buf = []
    i = 0
    while i < len(s):
        c = s[i: i+1]
        if c == b'\\':
            c2 = s[i+1: i+2]
            if c2 == b'\\':  # escaped \
                if PY2: c = str(c)
                i += 2
            else:   # A \xxx octal string
                c = chr(int(str(s[i+1: i+4]), 8)) if PY2 else bytes([int(s[i+1: i+4], 8)])
                i += 4
        else:
            if PY2: c = str(c)
            i += 1
        buf.append(c)
    return b''.join(buf)


# Type casting of SQL types bytes representation into Python objects
def vertica_type_cast(type_code, unicode_error):
    typecaster = {
        VerticaType.UNKNOWN: bytes,
        VerticaType.BOOL: lambda s: s == b't',
        VerticaType.INT8: lambda s: int(s),
        VerticaType.FLOAT8: lambda s: float(s),
        VerticaType.CHAR: lambda s: s.decode('utf-8', unicode_error),
        VerticaType.VARCHAR: lambda s: s.decode('utf-8', unicode_error),
        VerticaType.DATE: date_parse,
        VerticaType.TIME: time_parse,
        VerticaType.TIMESTAMP: timestamp_parse,
        VerticaType.TIMESTAMPTZ: timestamp_tz_parse,
        VerticaType.INTERVAL: bytes,
        VerticaType.TIMETZ: bytes,
        VerticaType.NUMERIC: lambda s: Decimal(s.decode('utf-8', unicode_error)),
        VerticaType.VARBINARY: binary_data_parse,
        VerticaType.UUID: lambda s: UUID(s.decode('utf-8', unicode_error)),
        VerticaType.INTERVALYM: bytes,
        VerticaType.LONGVARCHAR: lambda s: s.decode('utf-8', unicode_error),
        VerticaType.LONGVARBINARY: binary_data_parse,
        VerticaType.BINARY: binary_data_parse
    }
    return typecaster.get(type_code, bytes)


ColumnTuple = namedtuple('Column', ['name', 'type_code', 'display_size', 'internal_size',
                                    'precision', 'scale', 'null_ok'])


class Column(object):
    def __init__(self, col, unicode_error='strict'):
        # Describe one query result column
        self.name = col['name']
        self.type_code = col['data_type_oid']
        self.type_name = col['data_type_name']
        self.display_size = getDisplaySize(col['data_type_oid'], col['type_modifier'])
        self.internal_size = col['data_type_size']
        self.precision = getPrecision(col['data_type_oid'], col['type_modifier'])
        self.scale = getScale(col['data_type_oid'], col['type_modifier'])
        self.null_ok = col['null_ok']
        self.is_identity = col['is_identity']
        self.converter = vertica_type_cast(self.type_code, unicode_error)
        self.props = ColumnTuple(self.name, self.type_code, self.display_size, self.internal_size,
                                 self.precision, self.scale, self.null_ok)

    def convert(self, s):
        if s is None:
            return
        return self.converter(s)

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
