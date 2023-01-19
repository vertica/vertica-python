# Copyright (c) 2022-2023 Micro Focus or one of its affiliates.
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

import json
import re
from datetime import date, datetime, time, timedelta
from dateutil import tz
from dateutil.relativedelta import relativedelta
from decimal import Context, Decimal
from struct import unpack
from uuid import UUID

from .. import errors
from ..compat import as_str, as_bytes
from ..datatypes import VerticaType
from ..vertica.column import FormatCode


class Deserializer(object):
    def get_row_deserializers(self, columns, context):
        result = [None] * len(columns)
        for idx, col in enumerate(columns):
            result[idx] = self.get_column_deserializer(col, context)
        return result

    def get_column_deserializer(self, col, context):
        """Return a function that inputs a column's raw data and returns a Python object"""
        f = DEFAULTS.get(col.format_code, {}).get(col.type_code)
        if f is None:  # skip conversion
            return lambda data: data

        def deserializer(data):
            if data is None: # null
                return None
            return f(data, ctx={'column': col, **context})
        return deserializer


YEAR_TO_MONTH_RE = re.compile(r"(-)?(\d+)-(\d+)")
TIMETZ_RE = re.compile(
    r"""(?ix)
    ^
    (\d+) : (\d+) : (\d+) (?: \. (\d+) )?       # Time and micros
    ([-+]) (\d+) (?: : (\d+) )? (?: : (\d+) )?  # Timezone
    $
    """
)
TZ_RE = re.compile(r"(?ix) ^([-+]) (\d+) (?: : (\d+) )? (?: : (\d+) )? $")
SECONDS_PER_DAY = 86400

def load_bool_binary(val, ctx):
    """
    Parses binary representation of a BOOLEAN type.
    :param val: a byte - b'\x01' for True, b'\x00' for False
    :param ctx: dict
    :return: an instance of bool
    """
    return val == b'\x01'

def load_int8_binary(val, ctx):
    """
    Parses binary representation of a INTEGER type.
    :param val: bytes - a 64-bit integer.
    :param ctx: dict
    :return: int
    """
    return unpack("!q", val)[0]

def load_float8_binary(val, ctx):
    """
    Parses binary representation of a FLOAT type.
    :param val: bytes - a float encoded in IEEE-754 format.
    :param ctx: dict
    :return: float
    """
    return unpack("!d", val)[0]

def load_numeric_binary(val, ctx):
    """
    Parses binary representation of a NUMERIC type.
    :param val: bytes
    :param ctx: dict
    :return: decimal.Decimal
    """
    # N-byte signed integer represents the unscaled value of the numeric
    # N is roughly (precision//19+1)*8
    unscaledVal = int.from_bytes(val, byteorder='big', signed=True)
    precision = ctx['column'].precision
    scale = ctx['column'].scale
    # The numeric value is (unscaledVal * 10^(-scale))
    return Decimal(unscaledVal).scaleb(-scale, context=Context(prec=precision))

def load_varchar_text(val, ctx):
    """
    Parses text/binary representation of a CHAR / VARCHAR / LONG VARCHAR type.
    :param val: bytes
    :param ctx: dict
    :return: str
    """
    return val.decode('utf-8', ctx['unicode_error'])

def load_date_text(val, ctx):
    """
    Parses text representation of a DATE type.
    :param val: bytes
    :param ctx: dict
    :return: datetime.date
    :raises NotSupportedError when a date Before Christ is encountered
    """
    s = as_str(val)
    if s.endswith(' BC'):
        raise errors.NotSupportedError('Dates Before Christ are not supported by datetime.date. Got: {0}'.format(s))
    try:
        return date(*map(lambda x: int(x), s.split('-')))
    except ValueError:
        raise errors.NotSupportedError('Dates after year 9999 are not supported by datetime.date. Got: {0}'.format(s))

def load_date_binary(val, ctx):
    """
    Parses binary representation of a DATE type.
    :param val: bytes
    :param ctx: dict
    :return: datetime.date
    :raises NotSupportedError when a date Before Christ is encountered
    """
    # 8-byte integer represents the Julian day number
    # https://en.wikipedia.org/wiki/Julian_day
    jdn = load_int8_binary(val, ctx)
    days = jdn - 1721426 + 1  # shift epoch to 0001-1-1 (J1721426)
    if days < date.min.toordinal():
        raise errors.NotSupportedError('Dates Before Christ are not supported by datetime.date. Got: Julian day number {0}'.format(jdn))
    elif days > date.max.toordinal():
        raise errors.NotSupportedError('Dates after year 9999 are not supported by datetime.date. Got: Julian day number {0}'.format(jdn))
    return date.fromordinal(days)

def load_time_text(val, ctx):
    """
    Parses text representation of a TIME type.
    :param val: bytes
    :param ctx: dict
    :return: datetime.time
    """
    val = as_str(val)
    if len(val) == 8:
        return datetime.strptime(val, '%H:%M:%S').time()
    return datetime.strptime(val, '%H:%M:%S.%f').time()

def load_time_binary(val, ctx):
    """
    Parses binary representation of a TIME type.
    :param val: bytes
    :param ctx: dict
    :return: datetime.time
    """
    # 8-byte integer represents the number of microseconds
    # since midnight in the UTC time zone.
    msecs = load_int8_binary(val, ctx)

    msecs, fraction = divmod(msecs, 1000000)
    msecs, second = divmod(msecs, 60)
    hour, minute = divmod(msecs, 60)
    try:
        return time(hour, minute, second, fraction)
    except ValueError:
        raise errors.NotSupportedError("Time not supported by datetime.time. Got: hour={}".format(hour))

def load_timetz_text(val, ctx):
    """
    Parses text representation of a TIMETZ type.
    :param val: bytes
    :param ctx: dict
    :return: datetime.time
    """
    s = as_str(val)
    m = TIMETZ_RE.match(s)
    if not m:
        raise errors.DataError("Cannot parse time with time zone '{}'".format(s))
    hr, mi, sec, fr, sign, oh, om, os = m.groups()

    # Pad the fraction of second until it represents 6 digits
    us = 0
    if fr:
        pad = 6 - len(fr)
        us = int(fr) * (10**pad)

    # Calculate timezone
    # Note: before python version 3.7 timezone offset is restricted to a whole number of minutes
    #       tz.tzoffset() will round seconds in the offset to whole minutes
    tz_offset = 60 * 60 * int(oh)
    if om:
        tz_offset += 60 * int(om)
    if os:
        tz_offset += int(os)
    if sign == "-":
        tz_offset = -tz_offset

    return time(int(hr), int(mi), int(sec), us, tz.tzoffset(None, tz_offset))

def load_timetz_binary(val, ctx):
    """
    Parses binary representation of a TIMETZ type.
    :param val: bytes
    :param ctx: dict
    :return: datetime.time
    """
    # 8-byte value where
    #   - Upper 40 bits contain the number of microseconds since midnight in the UTC time zone.
    #   - Lower 24 bits contain time zone as the UTC offset in seconds.
    val = load_int8_binary(val, ctx)
    tz_offset = SECONDS_PER_DAY - (val & 0xffffff) # in seconds
    msecs = val >> 24
    # shift to given time zone
    msecs += tz_offset * 1000000
    msecs %= SECONDS_PER_DAY * 1000000
    msecs, fraction = divmod(msecs, 1000000)
    msecs, second = divmod(msecs, 60)
    hour, minute = divmod(msecs, 60)
    return time(hour, minute, second, fraction, tz.tzoffset(None, tz_offset))

def load_timestamp_text(val, ctx):
    """
    Parses text representation of a TIMESTAMP type.
    :param val: bytes
    :param ctx: dict
    :return: datetime.datetime
    """
    s = as_str(val)
    if s.endswith(" BC"):
        raise errors.NotSupportedError('Timestamps Before Christ are not supported by datetime.datetime. Got: {0}'.format(s))
    fmt = '%Y-%m-%d %H:%M:%S.%f' if '.' in s else '%Y-%m-%d %H:%M:%S'
    try:
        return datetime.strptime(s, fmt)
    except ValueError:
        raise errors.NotSupportedError('Timestamps after year 9999 are not supported by datetime.datetime. Got: {0}'.format(s))

def load_timestamp_binary(val, ctx):
    """
    Parses binary representation of a TIMESTAMP type.
    :param val: bytes
    :param ctx: dict
    :return: datetime.datetime
    """
    # 8-byte integer represents the number of microseconds since 2000-01-01 00:00:00.
    msecs = load_int8_binary(val, ctx)
    _datetime_epoch = datetime(2000, 1, 1)
    try:
        return _datetime_epoch + timedelta(microseconds=msecs)
    except OverflowError:
        if msecs < 0:
            raise errors.NotSupportedError('Timestamps Before Christ are not supported by datetime.datetime.')
        else:
            raise errors.NotSupportedError('Timestamps after year 9999 are not supported by datetime.datetime.')

def load_timestamptz_text(val, ctx):
    """
    Parses text representation of a TIMESTAMPTZ type.
    :param val: bytes
    :param ctx: dict
    :return: datetime.datetime
    """
    s = as_str(val)
    if s.endswith(" BC"):
        raise errors.NotSupportedError('TimestampTzs Before Christ are not supported by datetime.datetime. Got: {0}'.format(s))
    dt = s.split(' ')  # split into date part and time part
    if len(dt) != 2:
        raise errors.DataError("Cannot parse TIMESTAMPTZ '{}'".format(s))
    try:
        d = date(*map(lambda x: int(x), dt[0].split('-')))
    except ValueError:  # year might be over 9999
        raise errors.NotSupportedError('TimestampTzs after year 9999 are not supported by datetime.datetime. Got: {0}'.format(s))
    t = load_timetz_text(dt[1], ctx)
    return datetime.combine(d, t)

def load_timestamptz_binary(val, ctx):
    """
    Parses binary representation of a TIMESTAMPTZ type.
    :param val: bytes
    :param ctx: dict
    :return: datetime.datetime
    """
    # 8-byte integer represents the number of microseconds since 2000-01-01 00:00:00 in the UTC timezone.
    msecs = load_int8_binary(val, ctx)
    _datetimetz_epoch = datetime(2000, 1, 1, tzinfo=tz.tzutc())
    # Process session time zone setting
    if TZ_RE.match(ctx['session_tz']):  # -HH:MM / +HH:MM
        ctx['session_tz'] = 'UTC' + ctx['session_tz']
    session_tz = tz.gettz(ctx['session_tz'])
    # Use local time zone if session time zone is unknown
    timezone = session_tz if session_tz else tz.gettz()
    try:
        ts = _datetimetz_epoch + timedelta(microseconds=msecs)
        return ts.astimezone(timezone)
    except OverflowError:
        if msecs < 0:
            raise errors.NotSupportedError('TimestampTzs Before Christ are not supported by datetime.datetime.')
        else:  # year might be over 9999
            raise errors.NotSupportedError('TimestampTzs after year 9999 are not supported by datetime.datetime.')

def load_interval_text(val, ctx):
    """
    Parses text representation of a INTERVAL day-time type.
    :param val: bytes
    :param ctx: dict
    :return: dateutil.relativedelta.relativedelta
    """
    # [-]dd hh:mm:ss.ffffff
    interval = as_str(val)
    sign = -1 if interval[0] == '-' else 1
    parts = [0] * 5  # value of [day, hour, minute, second, fraction]

    sp = interval.split('.')
    if len(sp) > 1: # Extract the fractional second part
        fraction = sp[1]
        pad = 6 - len(fraction) # pad the fraction until it represents 6 digits
        parts[4] = sign * int(fraction) * (10**pad)
        interval = sp[0]

    # Extract the first number
    idx = 0
    while idx < len(interval) and interval[idx] not in (' ', ':'):
        idx += 1
    num = int(interval[:idx])
    saw_days = idx < len(interval) and interval[idx] == ' '
    idx += 1

    # Determine the unit for the first number
    type_name = ctx['column'].type_name
    parts_idx = 0  # Interval Day
    if type_name in ('Interval Day to Hour', 'Interval Day to Minute', 'Interval Day to Second'):
        parts_idx = 0 if (saw_days or idx > len(interval)) else 1
    elif type_name in ('Interval Hour', 'Interval Hour to Minute', 'Interval Hour to Second'):
        parts_idx = 1
    elif type_name in ('Interval Minute', 'Interval Minute to Second'):
        parts_idx = 2
    elif type_name == 'Interval Second':
        parts_idx = 3

    parts[parts_idx] = num  # Save the first number
    if idx < len(interval): # Parse the rest of interval
        parts_idx += 1
        ts = interval[idx:].split(':')
        for val in ts:
            parts[parts_idx] = sign * int(val)
            parts_idx += 1

    return relativedelta(days=parts[0], hours=parts[1], minutes=parts[2], seconds=parts[3], microseconds=parts[4])

def load_interval_binary(val, ctx):
    """
    Parses binary representation of a INTERVAL day-time type.
    :param val: bytes
    :param ctx: dict
    :return: dateutil.relativedelta.relativedelta
    """
    # 8-byte integer containing the number of microseconds in the interval
    msecs = load_int8_binary(val, ctx)
    return relativedelta(microseconds=msecs)

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
    :param ctx: dict
    :return: bytes
    """
    s = as_bytes(s)
    buf = []
    i = 0
    while i < len(s):
        c = s[i: i+1]
        if c == b'\\':
            c2 = s[i+1: i+2]
            if c2 == b'\\':  # escaped \
                i += 2
            else:   # A \xxx octal string
                c = bytes([int(s[i+1: i+4], 8)])
                i += 4
        else:
            i += 1
        buf.append(c)
    return b''.join(buf)

def load_array_text(val, ctx):
    """
    Parses text/binary representation of an ARRAY type.
    :param val: bytes
    :param ctx: dict
    :return: list
    """
    val = val.decode('utf-8', ctx['unicode_error'])
    # Some old servers have a bug of sending ARRAY oid without child metadata
    if not ctx['complex_types_enabled']:
        return val
    json_data = json.loads(val)
    return parse_array(json_data, ctx)

def load_set_text(val, ctx):
    """
    Parses text/binary representation of a SET type.
    :param val: bytes
    :param ctx: dict
    :return: set
    """
    return set(load_array_text(val, ctx))

def parse_array(json_data, ctx):
    if not isinstance(json_data, list):
        raise TypeError('Expected a list, got {}'.format(json_data))
    # An array has only one child, all elements in the array are the same type.
    child_ctx = ctx.copy()
    child_ctx['column'] = ctx['column'].child_columns[0]

    # Shortcut: return data parsed by the default JSONDecoder
    if child_ctx['column'].type_code in (VerticaType.BOOL, VerticaType.INT8,
                    VerticaType.CHAR, VerticaType.VARCHAR, VerticaType.LONGVARCHAR):
        return json_data

    parsed_array = [None] * len(json_data)
    for idx, element in enumerate(json_data):
        if element is None:
            continue
        parsed_array[idx] = parse_json_element(element, child_ctx)
    return parsed_array

def load_row_text(val, ctx):
    """
    Parses text/binary representation of a ROW type.
    :param val: bytes
    :param ctx: dict
    :return: dict
    """
    val = val.decode('utf-8', ctx['unicode_error'])
    # Some old servers have a bug of sending ROW oid without child metadata
    if not ctx['complex_types_enabled']:
        return val
    json_data = json.loads(val)
    return parse_row(json_data, ctx)

def parse_row(json_data, ctx):
    if not isinstance(json_data, dict):
        raise TypeError('Expected a dict, got {}'.format(json_data))
    # A row has one or more child fields
    child_columns = ctx['column'].child_columns
    if child_columns is None:   # Special case: SELECT ROW();
        return json_data
    if len(json_data) != len(child_columns): # This situation should never occur
        raise ValueError('The metadata does not match the fields in the ROW.')
    parsed_row = {}
    for child_column in child_columns:
        key = child_column.name
        element = json_data[key]
        if element is None:
            parsed_row[key] = None
            continue
        child_ctx = ctx.copy()
        child_ctx['column'] = child_column
        parsed_row[key] = parse_json_element(element, child_ctx)
    return parsed_row

def parse_json_element(element, ctx):
    type_code = ctx['column'].type_code
    if type_code in (VerticaType.BOOL, VerticaType.INT8,
                     VerticaType.CHAR, VerticaType.VARCHAR, VerticaType.LONGVARCHAR):
        return element
    # "-Infinity", "Infinity", "NaN"
    if type_code == VerticaType.FLOAT8:
        return float(element)
    # element type: str
    if type_code in (VerticaType.DATE, VerticaType.TIME, VerticaType.TIMETZ,
                     VerticaType.TIMESTAMP, VerticaType.TIMESTAMPTZ,
                     VerticaType.INTERVAL, VerticaType.INTERVALYM,
                     VerticaType.BINARY, VerticaType.VARBINARY,
                     VerticaType.LONGVARBINARY):
        return DEFAULTS[FormatCode.TEXT][type_code](element, ctx)
    elif type_code == VerticaType.NUMERIC:
        return Decimal(element)
    elif type_code == VerticaType.UUID:
        return UUID(element)
    # element type: list
    elif type_code == VerticaType.ARRAY:
        return parse_array(element, ctx)
    # element type: dict
    elif type_code == VerticaType.ROW:
        return parse_row(element, ctx)
    return element

DEFAULTS = {
    FormatCode.TEXT: {
        VerticaType.UNKNOWN: None,
        VerticaType.BOOL: lambda val, ctx: val == b't',
        VerticaType.INT8: lambda val, ctx: int(val),
        VerticaType.FLOAT8: lambda val, ctx: float(val),
        VerticaType.NUMERIC: lambda val, ctx: Decimal(val.decode('utf-8')),
        VerticaType.CHAR: load_varchar_text,
        VerticaType.VARCHAR: load_varchar_text,
        VerticaType.LONGVARCHAR: load_varchar_text,
        VerticaType.DATE: load_date_text,
        VerticaType.TIME: load_time_text,
        VerticaType.TIMETZ: load_timetz_text,
        VerticaType.TIMESTAMP: load_timestamp_text,
        VerticaType.TIMESTAMPTZ: load_timestamptz_text,
        VerticaType.INTERVAL: load_interval_text,
        VerticaType.INTERVALYM: load_intervalYM_text,
        VerticaType.UUID: lambda val, ctx: UUID(val.decode('utf-8')),
        VerticaType.BINARY: load_varbinary_text,
        VerticaType.VARBINARY: load_varbinary_text,
        VerticaType.LONGVARBINARY: load_varbinary_text,
        VerticaType.ARRAY: load_array_text,
        VerticaType.ARRAY1D_BOOL: load_array_text,
        VerticaType.ARRAY1D_INT8: load_array_text,
        VerticaType.ARRAY1D_FLOAT8: load_array_text,
        VerticaType.ARRAY1D_NUMERIC: load_array_text,
        VerticaType.ARRAY1D_CHAR: load_array_text,
        VerticaType.ARRAY1D_VARCHAR: load_array_text,
        VerticaType.ARRAY1D_LONGVARCHAR: load_array_text,
        VerticaType.ARRAY1D_DATE: load_array_text,
        VerticaType.ARRAY1D_TIME: load_array_text,
        VerticaType.ARRAY1D_TIMETZ: load_array_text,
        VerticaType.ARRAY1D_TIMESTAMP: load_array_text,
        VerticaType.ARRAY1D_TIMESTAMPTZ: load_array_text,
        VerticaType.ARRAY1D_INTERVAL: load_array_text,
        VerticaType.ARRAY1D_INTERVALYM: load_array_text,
        VerticaType.ARRAY1D_UUID: load_array_text,
        VerticaType.ARRAY1D_BINARY: load_array_text,
        VerticaType.ARRAY1D_VARBINARY: load_array_text,
        VerticaType.ARRAY1D_LONGVARBINARY: load_array_text,
        VerticaType.SET_BOOL: load_set_text,
        VerticaType.SET_INT8: load_set_text,
        VerticaType.SET_FLOAT8: load_set_text,
        VerticaType.SET_CHAR: load_set_text,
        VerticaType.SET_VARCHAR: load_set_text,
        VerticaType.SET_DATE: load_set_text,
        VerticaType.SET_TIME: load_set_text,
        VerticaType.SET_TIMESTAMP: load_set_text,
        VerticaType.SET_TIMESTAMPTZ: load_set_text,
        VerticaType.SET_TIMETZ: load_set_text,
        VerticaType.SET_INTERVAL: load_set_text,
        VerticaType.SET_INTERVALYM: load_set_text,
        VerticaType.SET_NUMERIC: load_set_text,
        VerticaType.SET_VARBINARY: load_set_text,
        VerticaType.SET_UUID: load_set_text,
        VerticaType.SET_BINARY: load_set_text,
        VerticaType.SET_LONGVARCHAR: load_set_text,
        VerticaType.SET_LONGVARBINARY: load_set_text,
        VerticaType.ROW: load_row_text,
        VerticaType.MAP: load_row_text,
    },
    FormatCode.BINARY: {
        VerticaType.UNKNOWN: None,
        VerticaType.BOOL: load_bool_binary,
        VerticaType.INT8: load_int8_binary,
        VerticaType.FLOAT8: load_float8_binary,
        VerticaType.NUMERIC: load_numeric_binary,
        VerticaType.CHAR: load_varchar_text,
        VerticaType.VARCHAR: load_varchar_text,
        VerticaType.LONGVARCHAR: load_varchar_text,
        VerticaType.DATE: load_date_binary,
        VerticaType.TIME: load_time_binary,
        VerticaType.TIMETZ: load_timetz_binary,
        VerticaType.TIMESTAMP: load_timestamp_binary,
        VerticaType.TIMESTAMPTZ: load_timestamptz_binary,
        VerticaType.INTERVAL: load_interval_binary,
        VerticaType.INTERVALYM: load_intervalYM_binary,
        VerticaType.UUID: load_uuid_binary,
        VerticaType.BINARY: None,
        VerticaType.VARBINARY: None,
        VerticaType.LONGVARBINARY: None,
        VerticaType.ARRAY: load_array_text,
        VerticaType.ARRAY1D_BOOL: load_array_text,
        VerticaType.ARRAY1D_INT8: load_array_text,
        VerticaType.ARRAY1D_FLOAT8: load_array_text,
        VerticaType.ARRAY1D_NUMERIC: load_array_text,
        VerticaType.ARRAY1D_CHAR: load_array_text,
        VerticaType.ARRAY1D_VARCHAR: load_array_text,
        VerticaType.ARRAY1D_LONGVARCHAR: load_array_text,
        VerticaType.ARRAY1D_DATE: load_array_text,
        VerticaType.ARRAY1D_TIME: load_array_text,
        VerticaType.ARRAY1D_TIMETZ: load_array_text,
        VerticaType.ARRAY1D_TIMESTAMP: load_array_text,
        VerticaType.ARRAY1D_TIMESTAMPTZ: load_array_text,
        VerticaType.ARRAY1D_INTERVAL: load_array_text,
        VerticaType.ARRAY1D_INTERVALYM: load_array_text,
        VerticaType.ARRAY1D_UUID: load_array_text,
        VerticaType.ARRAY1D_BINARY: load_array_text,
        VerticaType.ARRAY1D_VARBINARY: load_array_text,
        VerticaType.ARRAY1D_LONGVARBINARY: load_array_text,
        VerticaType.SET_BOOL: load_set_text,
        VerticaType.SET_INT8: load_set_text,
        VerticaType.SET_FLOAT8: load_set_text,
        VerticaType.SET_CHAR: load_set_text,
        VerticaType.SET_VARCHAR: load_set_text,
        VerticaType.SET_DATE: load_set_text,
        VerticaType.SET_TIME: load_set_text,
        VerticaType.SET_TIMESTAMP: load_set_text,
        VerticaType.SET_TIMESTAMPTZ: load_set_text,
        VerticaType.SET_TIMETZ: load_set_text,
        VerticaType.SET_INTERVAL: load_set_text,
        VerticaType.SET_INTERVALYM: load_set_text,
        VerticaType.SET_NUMERIC: load_set_text,
        VerticaType.SET_VARBINARY: load_set_text,
        VerticaType.SET_UUID: load_set_text,
        VerticaType.SET_BINARY: load_set_text,
        VerticaType.SET_LONGVARCHAR: load_set_text,
        VerticaType.SET_LONGVARBINARY: load_set_text,
        VerticaType.ROW: load_row_text,
        VerticaType.MAP: load_row_text,
    },
}

