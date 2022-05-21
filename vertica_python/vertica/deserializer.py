# Copyright (c) 2022 Micro Focus or one of its affiliates.
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

from __future__ import print_function, division, absolute_import

import re
from dateutil.relativedelta import relativedelta
from six import PY2
from struct import unpack
from uuid import UUID

from ..compat import as_str
from ..datatypes import VerticaType
from ..vertica.column import FormatCode


class Deserializer(object):
    def get_row_deserializers(self, columns):
        result = [None] * len(columns)
        for idx, col in enumerate(columns):
            result[idx] = self.get_column_deserializer(col)
        return result

    def get_column_deserializer(self, col):
        """Return a function that inputs a column's raw data and returns a Python object"""
        def deserializer(data):
            if data is None: # null
                return None
            f = DEFAULTS.get(col.format_code, {}).get(col.type_code)
            if f:
                return f(data, ctx={'column': col})
            return data  # skip
        return deserializer


YEAR_TO_MONTH_RE = re.compile(r"(-)?(\d+)-(\d+)")

def load_bool_binary(val, ctx):
    """
    Parses binary representation of a BOOLEAN type.
    :param val: a byte - b'\x01' for True, b'\x00' for False
    :return: an instance of bool
    """
    return val == b'\x01'

def load_int8_binary(val, ctx):
    return unpack("!q", val)[0]

def load_float8_binary(val, ctx):
    """
    Parses binary representation of a FLOAT type.
    :param val: bytes
    :param ctx: dict
    :return: float
    """
    return unpack("!d", val)[0]

def load_intervalYM_text(val, ctx):
    """
    Parses text representation of a INTERVAL YEAR TO MONTH / INTERVAL YEAR / INTERVAL MONTH type.
    :param val: bytes
    :param ctx: dict
    :return: dateutil.relativedelta.relativedelta
    """
    s = as_str(val)
    type_name = ctx['column'].type_name
    if type_name == 'Interval Year to Month':
        m = YEAR_TO_MONTH_RE.match(s)
        if not m:
            raise errors.DataError("Cannot parse interval '{}'".format(s))
        sign, year, month = m.groups()
        sign = -1 if sign else 1
        return relativedelta(years=sign*int(year), months=sign*int(month))
    else:
        try:
            interval = int(s)
        except ValueError:
            raise errors.DataError("Cannot parse interval '{}'".format(s))
        if type_name == 'Interval Year':
            return relativedelta(years=interval)
        else:   # Interval Month
            return relativedelta(months=interval)

def load_intervalYM_binary(val, ctx):
    """
    Parses binary representation of a INTERVAL YEAR TO MONTH / INTERVAL YEAR / INTERVAL MONTH type.
    :param val: bytes
    :param ctx: dict
    :return: dateutil.relativedelta.relativedelta
    """
    # 8-byte integer containing the number of months in the interval
    months = load_int8_binary(val, ctx)
    return relativedelta(months=months)

def load_uuid_binary(val, ctx):
    """
    Parses binary representation of a UUID type.
    :param val: bytes
    :param ctx: dict
    :return: uuid.UUID
    """
    # 16-byte value in big-endian order interpreted as UUID
    return UUID(bytes=bytes(val))

def load_varbinary_text(s, ctx):
    """
    Parses text representation of a BINARY / VARBINARY / LONG VARBINARY type.
    :param s: bytes
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


DEFAULTS = {
    FormatCode.TEXT: {
        VerticaType.UNKNOWN: None,
        VerticaType.BOOL: lambda s, ctx: s == b't',
        VerticaType.INT8: lambda s, ctx: int(s),
        VerticaType.FLOAT8: lambda s, ctx: float(s),

        VerticaType.INTERVALYM: load_intervalYM_text,
        VerticaType.UUID: lambda val, ctx: UUID(val.decode('utf-8')),
        VerticaType.BINARY: load_varbinary_text,
        VerticaType.VARBINARY: load_varbinary_text,
        VerticaType.LONGVARBINARY: load_varbinary_text,
    },
    FormatCode.BINARY:{
        VerticaType.UNKNOWN: None,
        VerticaType.BOOL: load_bool_binary,
        VerticaType.INT8: load_int8_binary,
        VerticaType.FLOAT8: load_float8_binary,

        VerticaType.INTERVALYM: load_intervalYM_binary,
        VerticaType.UUID: load_uuid_binary,
        VerticaType.BINARY: None,
        VerticaType.VARBINARY: None,
        VerticaType.LONGVARBINARY: None,
    },
}

