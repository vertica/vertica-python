# Copyright (c) 2018-2023 Micro Focus or one of its affiliates.
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

from datetime import date, datetime, time


# noinspection PyPep8Naming
def Date(year, month, day):
    return date(year, month, day)


# noinspection PyPep8Naming
def Time(hour, minute, second):
    return time(hour, minute, second)


# noinspection PyPep8Naming
def Timestamp(year, month, day, hour, minute, second):
    return datetime(year, month, day, hour, minute, second)


# noinspection PyPep8Naming
def DateFromTicks(ticks):
    d = datetime.utcfromtimestamp(ticks)
    return d.date()


# noinspection PyPep8Naming
def TimeFromTicks(ticks):
    d = datetime.utcfromtimestamp(ticks)
    return d.time()


# noinspection PyPep8Naming
def TimestampFromTicks(ticks):
    d = datetime.utcfromtimestamp(ticks)
    return d.time()


class Bytea(str):
    pass


# noinspection PyPep8Naming
def Binary(string):
    return Bytea(string)


class VerticaType(object):
    UNKNOWN = 4
    BOOL = 5
    INT8 = 6
    FLOAT8 = 7
    CHAR = 8
    VARCHAR = 9
    DATE = 10
    TIME = 11
    TIMESTAMP = 12
    TIMESTAMPTZ = 13
    INTERVAL = 14
    INTERVALYM = 114
    TIMETZ = 15
    NUMERIC = 16
    VARBINARY = 17
    UUID = 20
    LONGVARCHAR = 115
    LONGVARBINARY = 116
    BINARY = 117

    ROW = 300
    ARRAY = 301 # multidimensional array or contain ROWs
    MAP = 302

    # one-dimensional array of a primitive type
    ARRAY1D_BOOL = 1505
    ARRAY1D_INT8 = 1506
    ARRAY1D_FLOAT8 = 1507
    ARRAY1D_CHAR = 1508
    ARRAY1D_VARCHAR = 1509
    ARRAY1D_DATE = 1510
    ARRAY1D_TIME = 1511
    ARRAY1D_TIMESTAMP = 1512
    ARRAY1D_TIMESTAMPTZ = 1513
    ARRAY1D_INTERVAL = 1514
    ARRAY1D_INTERVALYM = 1521
    ARRAY1D_TIMETZ = 1515
    ARRAY1D_NUMERIC = 1516
    ARRAY1D_VARBINARY = 1517
    ARRAY1D_UUID = 1520
    ARRAY1D_BINARY = 1522
    ARRAY1D_LONGVARCHAR = 1519
    ARRAY1D_LONGVARBINARY = 1518

    # one-dimensional set of a primitive type
    SET_BOOL = 2705
    SET_INT8 = 2706
    SET_FLOAT8 = 2707
    SET_CHAR = 2708
    SET_VARCHAR = 2709
    SET_DATE = 2710
    SET_TIME = 2711
    SET_TIMESTAMP = 2712
    SET_TIMESTAMPTZ = 2713
    SET_INTERVAL = 2714
    SET_INTERVALYM = 2721
    SET_TIMETZ = 2715
    SET_NUMERIC = 2716
    SET_VARBINARY = 2717
    SET_UUID = 2720
    SET_BINARY = 2722
    SET_LONGVARCHAR = 2719
    SET_LONGVARBINARY = 2718

    def __init__(self, *values):
        self.values = values

    def __cmp__(self, other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1

    def __eq__(self, other):
        return other in self.values

    def __ne__(self, other):
        return other not in self.values


# dbapi: type object used to describe columns that are string-based
STRING = VerticaType(VerticaType.CHAR, VerticaType.VARCHAR, VerticaType.BINARY,
                     VerticaType.VARBINARY, VerticaType.UNKNOWN,
                     VerticaType.LONGVARBINARY, VerticaType.LONGVARCHAR)
# dbapi: type object used to describe (long) binary columns
BINARY = VerticaType(VerticaType.BINARY, VerticaType.VARBINARY,
                     VerticaType.LONGVARBINARY)
# dbapi: type object used to describe numeric columns
NUMBER = VerticaType(VerticaType.INT8, VerticaType.FLOAT8, VerticaType.NUMERIC)
# dbapi: type object used to describe date/time columns
DATETIME = VerticaType(VerticaType.DATE, VerticaType.TIME, VerticaType.TIMETZ,
                       VerticaType.TIMESTAMP, VerticaType.TIMESTAMPTZ,
                       VerticaType.INTERVAL, VerticaType.INTERVALYM)
# dbapi: type object used to describe the "Row ID" column
ROWID = VerticaType()  # Vertica doesn't have row_id type

# the max size of a CHAR/VARCHAR/BINARY/VARBINARY
MAX_STRING_LEN = 65000
# the max size of a LONG VARCHAR/LONG VARBINARY
MAX_LONG_STRING_LEN = 32000000

# interval mask constants
# these masks determine the range of an interval for a given type_modifier
# for example, an interval has range "Day to Hour" if:
# (type_modifier & INTERVAL_MASK_DAY2HOUR) == INTERVAL_MASK_DAY2HOUR
INTERVAL_MASK_MONTH = 1 << 17
INTERVAL_MASK_YEAR = 1 << 18
INTERVAL_MASK_DAY = 1 << 19
INTERVAL_MASK_HOUR = 1 << 26
INTERVAL_MASK_MINUTE = 1 << 27
INTERVAL_MASK_SECOND = 1 << 28
INTERVAL_MASK_YEAR2MONTH = INTERVAL_MASK_YEAR | INTERVAL_MASK_MONTH
INTERVAL_MASK_DAY2HOUR = INTERVAL_MASK_DAY | INTERVAL_MASK_HOUR
INTERVAL_MASK_DAY2MIN = INTERVAL_MASK_DAY | INTERVAL_MASK_HOUR | INTERVAL_MASK_MINUTE
INTERVAL_MASK_DAY2SEC = INTERVAL_MASK_DAY | INTERVAL_MASK_HOUR | INTERVAL_MASK_MINUTE | INTERVAL_MASK_SECOND
INTERVAL_MASK_HOUR2MIN = INTERVAL_MASK_HOUR | INTERVAL_MASK_MINUTE
INTERVAL_MASK_HOUR2SEC = INTERVAL_MASK_HOUR | INTERVAL_MASK_MINUTE | INTERVAL_MASK_SECOND
INTERVAL_MASK_MIN2SEC = INTERVAL_MASK_MINUTE | INTERVAL_MASK_SECOND

TYPENAME = {
    VerticaType.UNKNOWN: "Unknown",
    VerticaType.BOOL: "Boolean",
    VerticaType.INT8: "Integer",
    VerticaType.FLOAT8: "Float",
    VerticaType.CHAR: "Char",
    VerticaType.VARCHAR: "Varchar",
    VerticaType.LONGVARCHAR: "Long Varchar",
    VerticaType.DATE: "Date",
    VerticaType.TIME: "Time",
    VerticaType.TIMETZ: "TimeTz",
    VerticaType.TIMESTAMP: "Timestamp",
    VerticaType.TIMESTAMPTZ: "TimestampTz",
    VerticaType.BINARY: "Binary",
    VerticaType.VARBINARY: "Varbinary",
    VerticaType.LONGVARBINARY: "Long Varbinary",
    VerticaType.NUMERIC: "Numeric",
    VerticaType.UUID: "Uuid",
    VerticaType.ROW: "Row",
    VerticaType.ARRAY: "Array",
    VerticaType.MAP: "Map",
    VerticaType.ARRAY1D_BOOL: "Array[Boolean]",
    VerticaType.ARRAY1D_INT8: "Array[Int8]",
    VerticaType.ARRAY1D_FLOAT8: "Array[Float8]",
    VerticaType.ARRAY1D_CHAR: "Array[Char]",
    VerticaType.ARRAY1D_VARCHAR: "Array[Varchar]",
    VerticaType.ARRAY1D_DATE: "Array[Date]",
    VerticaType.ARRAY1D_TIME: "Array[Time]",
    VerticaType.ARRAY1D_TIMESTAMP: "Array[Timestamp]",
    VerticaType.ARRAY1D_TIMESTAMPTZ: "Array[TimestampTz]",
    VerticaType.ARRAY1D_TIMETZ: "Array[TimeTz]",
    VerticaType.ARRAY1D_NUMERIC: "Array[Numeric]",
    VerticaType.ARRAY1D_VARBINARY: "Array[Varbinary]",
    VerticaType.ARRAY1D_UUID: "Array[Uuid]",
    VerticaType.ARRAY1D_BINARY: "Array[Binary]",
    VerticaType.ARRAY1D_LONGVARCHAR: "Array[Long Varchar]",
    VerticaType.ARRAY1D_LONGVARBINARY: "Array[Long Varbinary]",
    VerticaType.SET_BOOL: "Set[Boolean]",
    VerticaType.SET_INT8: "Set[Int8]",
    VerticaType.SET_FLOAT8: "Set[Float8]",
    VerticaType.SET_CHAR: "Set[Char]",
    VerticaType.SET_VARCHAR: "Set[Varchar]",
    VerticaType.SET_DATE: "Set[Date]",
    VerticaType.SET_TIME: "Set[Time]",
    VerticaType.SET_TIMESTAMP: "Set[Timestamp]",
    VerticaType.SET_TIMESTAMPTZ: "Set[TimestampTz]",
    VerticaType.SET_TIMETZ: "Set[TimeTz]",
    VerticaType.SET_NUMERIC: "Set[Numeric]",
    VerticaType.SET_VARBINARY: "Set[Varbinary]",
    VerticaType.SET_UUID: "Set[Uuid]",
    VerticaType.SET_BINARY: "Set[Binary]",
    VerticaType.SET_LONGVARCHAR: "Set[Long Varchar]",
    VerticaType.SET_LONGVARBINARY: "Set[Long Varbinary]",
}

COMPLEX_ELEMENT_TYPE = {
    VerticaType.ARRAY1D_BOOL: VerticaType.BOOL,
    VerticaType.ARRAY1D_INT8: VerticaType.INT8,
    VerticaType.ARRAY1D_FLOAT8: VerticaType.FLOAT8,
    VerticaType.ARRAY1D_CHAR: VerticaType.CHAR,
    VerticaType.ARRAY1D_VARCHAR: VerticaType.VARCHAR,
    VerticaType.ARRAY1D_DATE: VerticaType.DATE,
    VerticaType.ARRAY1D_TIME: VerticaType.TIME,
    VerticaType.ARRAY1D_TIMESTAMP: VerticaType.TIMESTAMP,
    VerticaType.ARRAY1D_TIMESTAMPTZ: VerticaType.TIMESTAMPTZ,
    VerticaType.ARRAY1D_TIMETZ: VerticaType.TIMETZ,
    VerticaType.ARRAY1D_INTERVAL: VerticaType.INTERVAL,
    VerticaType.ARRAY1D_INTERVALYM: VerticaType.INTERVALYM,
    VerticaType.ARRAY1D_NUMERIC: VerticaType.NUMERIC,
    VerticaType.ARRAY1D_VARBINARY: VerticaType.VARBINARY,
    VerticaType.ARRAY1D_UUID: VerticaType.UUID,
    VerticaType.ARRAY1D_BINARY: VerticaType.BINARY,
    VerticaType.ARRAY1D_LONGVARCHAR: VerticaType.LONGVARCHAR,
    VerticaType.ARRAY1D_LONGVARBINARY: VerticaType.LONGVARBINARY,
    VerticaType.SET_BOOL: VerticaType.BOOL,
    VerticaType.SET_INT8: VerticaType.INT8,
    VerticaType.SET_FLOAT8: VerticaType.FLOAT8,
    VerticaType.SET_CHAR: VerticaType.CHAR,
    VerticaType.SET_VARCHAR: VerticaType.VARCHAR,
    VerticaType.SET_DATE: VerticaType.DATE,
    VerticaType.SET_TIME: VerticaType.TIME,
    VerticaType.SET_TIMESTAMP: VerticaType.TIMESTAMP,
    VerticaType.SET_TIMESTAMPTZ: VerticaType.TIMESTAMPTZ,
    VerticaType.SET_TIMETZ: VerticaType.TIMETZ,
    VerticaType.SET_INTERVAL: VerticaType.INTERVAL,
    VerticaType.SET_INTERVALYM: VerticaType.INTERVALYM,
    VerticaType.SET_NUMERIC: VerticaType.NUMERIC,
    VerticaType.SET_VARBINARY: VerticaType.VARBINARY,
    VerticaType.SET_UUID: VerticaType.UUID,
    VerticaType.SET_BINARY: VerticaType.BINARY,
    VerticaType.SET_LONGVARCHAR: VerticaType.LONGVARCHAR,
    VerticaType.SET_LONGVARBINARY: VerticaType.LONGVARBINARY,
}

def getTypeName(data_type_oid, type_modifier):
    """Returns the base type name according to data_type_oid and type_modifier"""
    if data_type_oid in TYPENAME:
        return TYPENAME[data_type_oid]
    elif data_type_oid in (VerticaType.INTERVAL, VerticaType.INTERVALYM):
        return "Interval " + getIntervalRange(data_type_oid, type_modifier)
    elif data_type_oid in (VerticaType.ARRAY1D_INTERVAL, VerticaType.ARRAY1D_INTERVALYM):
        return "Array[Interval {}]".format(getIntervalRange(COMPLEX_ELEMENT_TYPE[data_type_oid], type_modifier))
    elif data_type_oid in (VerticaType.SET_INTERVAL, VerticaType.SET_INTERVALYM):
        return "Set[Interval {}]".format(getIntervalRange(COMPLEX_ELEMENT_TYPE[data_type_oid], type_modifier))
    else:
        return "Unknown"

def getComplexElementType(data_type_oid):
    """For 1D ARRAY or SET, returns the type of its elements"""
    return COMPLEX_ELEMENT_TYPE.get(data_type_oid)

def getIntervalRange(data_type_oid, type_modifier):
    """Extracts an interval's range from the bits set in its type_modifier"""

    if data_type_oid not in (VerticaType.INTERVAL, VerticaType.INTERVALYM):
        raise ValueError("Invalid data type OID: {}".format(data_type_oid))

    if type_modifier == -1:   # assume the default
        if data_type_oid == VerticaType.INTERVALYM:
            return "Year to Month"
        elif data_type_oid == VerticaType.INTERVAL:
            return "Day to Second"

    if data_type_oid == VerticaType.INTERVALYM:  # Year/Month intervals
        if (type_modifier & INTERVAL_MASK_YEAR2MONTH) == INTERVAL_MASK_YEAR2MONTH:
            return "Year to Month"
        elif (type_modifier & INTERVAL_MASK_YEAR) == INTERVAL_MASK_YEAR:
            return "Year"
        elif (type_modifier & INTERVAL_MASK_MONTH) == INTERVAL_MASK_MONTH:
            return "Month"
        else:
            return "Year to Month"

    if data_type_oid == VerticaType.INTERVAL:  # Day/Time intervals
        if (type_modifier & INTERVAL_MASK_DAY2SEC) == INTERVAL_MASK_DAY2SEC:
            return "Day to Second"
        elif (type_modifier & INTERVAL_MASK_DAY2MIN) == INTERVAL_MASK_DAY2MIN:
            return "Day to Minute"
        elif (type_modifier & INTERVAL_MASK_DAY2HOUR) == INTERVAL_MASK_DAY2HOUR:
            return "Day to Hour"
        elif (type_modifier & INTERVAL_MASK_DAY) == INTERVAL_MASK_DAY:
            return "Day"
        elif (type_modifier & INTERVAL_MASK_HOUR2SEC) == INTERVAL_MASK_HOUR2SEC:
            return "Hour to Second"
        elif (type_modifier & INTERVAL_MASK_HOUR2MIN) == INTERVAL_MASK_HOUR2MIN:
            return "Hour to Minute"
        elif (type_modifier & INTERVAL_MASK_HOUR) == INTERVAL_MASK_HOUR:
            return "Hour"
        elif (type_modifier & INTERVAL_MASK_MIN2SEC) == INTERVAL_MASK_MIN2SEC:
            return "Minute to Second"
        elif (type_modifier & INTERVAL_MASK_MINUTE) == INTERVAL_MASK_MINUTE:
            return "Minute"
        elif (type_modifier & INTERVAL_MASK_SECOND) == INTERVAL_MASK_SECOND:
            return "Second"
        else:
            return "Day to Second"


def getIntervalLeadingPrecision(data_type_oid, type_modifier):
    """
    Returns the leading precision for an interval, which is the largest number
    of digits that can fit in the leading field of the interval.

    All Year/Month intervals are defined in terms of months, even if the
    type_modifier forbids months to be specified (i.e. INTERVAL YEAR).
    Similarly, all Day/Time intervals are defined as a number of microseconds.
    Because of this, interval types with shorter ranges will have a larger
    leading precision.

    For example, an INTERVAL DAY's leading precision is
    ((2^63)-1)/MICROSECS_PER_DAY, while an INTERVAL HOUR's leading precision
    is ((2^63)-1)/MICROSECS_PER_HOUR
    """

    interval_range = getIntervalRange(data_type_oid, type_modifier)
    if interval_range in ("Year", "Year to Month"):
        return 18
    elif interval_range == "Month":
        return 19
    elif interval_range in ("Day", "Day to Hour", "Day to Minute", "Day to Second"):
        return 9
    elif interval_range in ("Hour", "Hour to Minute", "Hour to Second"):
        return 10
    elif interval_range in ("Minute", "Minute to Second"):
        return 12
    elif interval_range == "Second":
        return 13
    else:
        raise ValueError("Invalid interval range: {}".format(interval_range))


def getPrecision(data_type_oid, type_modifier):
    """
    Returns the precision for the given Vertica type with consideration of
    the type modifier.

    For numerics, precision is the total number of digits (in base 10) that can
    fit in the type.

    For intervals, time and timestamps, precision is the number of digits to the
    right of the decimal point in the seconds portion of the time.

    The type modifier of -1 is used when the size of a type is unknown. In those
    cases we assume the maximum possible size.
    """

    if data_type_oid == VerticaType.NUMERIC:
        if type_modifier == -1:
            return 1024
        return ((type_modifier - 4) >> 16) & 0xFFFF
    elif data_type_oid in (VerticaType.TIME, VerticaType.TIMETZ,
                           VerticaType.TIMESTAMP, VerticaType.TIMESTAMPTZ,
                           VerticaType.INTERVAL, VerticaType.INTERVALYM):
        if type_modifier == -1:
            return 6
        return type_modifier & 0xF
    else:
        return None  # None if no meaningful values can be provided


def getScale(data_type_oid, type_modifier):
    """
    Returns the scale for the given Vertica type with consideration of
    the type modifier.
    """

    if data_type_oid == VerticaType.NUMERIC:
        return 15 if type_modifier == -1 else (type_modifier - 4) & 0xFF
    else:
        return None  # None if no meaningful values can be provided


def getDisplaySize(data_type_oid, type_modifier):
    """
    Returns the column display size for the given Vertica type with
    consideration of the type modifier.

    The display size of a column is the maximum number of characters needed to
    display data in character form.
    """

    if data_type_oid == VerticaType.BOOL:
        # T or F
        return 1
    elif data_type_oid == VerticaType.INT8:
        # a sign and 19 digits if signed or 20 digits if unsigned
        return 20
    elif data_type_oid == VerticaType.FLOAT8:
        # a sign, 15 digits, a decimal point, the letter E, a sign, and 3 digits
        return 22
    elif data_type_oid == VerticaType.NUMERIC:
        # a sign, precision digits, and a decimal point
        return getPrecision(data_type_oid, type_modifier) + 2
    elif data_type_oid == VerticaType.DATE:
        # yyyy-mm-dd, a space, and the calendar era (BC)
        return 13
    elif data_type_oid == VerticaType.TIME:
        seconds_precision = getPrecision(data_type_oid, type_modifier)
        if seconds_precision == 0:
            # hh:mm:ss
            return 8
        else:
            # hh:mm:ss.[fff...]
            return 9 + seconds_precision
    elif data_type_oid == VerticaType.TIMETZ:
        seconds_precision = getPrecision(data_type_oid, type_modifier)
        if seconds_precision == 0:
            # hh:mm:ss, a sign, hh:mm
            return 14
        else:
            # hh:mm:ss.[fff...], a sign, hh:mm
            return 15 + seconds_precision
    elif data_type_oid == VerticaType.TIMESTAMP:
        seconds_precision = getPrecision(data_type_oid, type_modifier)
        if seconds_precision == 0:
            # yyyy-mm-dd hh:mm:ss, a space, and the calendar era (BC)
            return 22
        else:
            # yyyy-mm-dd hh:mm:ss[.fff...], a space, and the calendar era (BC)
            return 23 + seconds_precision
    elif data_type_oid == VerticaType.TIMESTAMPTZ:
        seconds_precision = getPrecision(data_type_oid, type_modifier)
        if seconds_precision == 0:
            # yyyy-mm-dd hh:mm:ss, a sign, hh:mm, a space, and the calendar era (BC)
            return 28
        else:
            # yyyy-mm-dd hh:mm:ss.[fff...], a sign, hh:mm, a space, and the calendar era (BC)
            return 29 + seconds_precision
    elif data_type_oid in (VerticaType.INTERVAL, VerticaType.INTERVALYM):
        leading_precision = getIntervalLeadingPrecision(data_type_oid, type_modifier)
        seconds_precision = getPrecision(data_type_oid, type_modifier)
        interval_range = getIntervalRange(data_type_oid, type_modifier)
        if interval_range in ("Year", "Month", "Day", "Hour", "Minute"):
            # a sign, [range...]
            return 1 + leading_precision
        elif interval_range in ("Day to Hour", "Year to Month", "Hour to Minute"):
            # a sign, [dd...] hh; a sign, [yy...]-mm; a sign, [hh...]:mm
            return 1 + leading_precision + 3
        elif interval_range == "Day to Minute":
            # a sign, [dd...] hh:mm
            return 1 + leading_precision + 6
        elif interval_range == "Second":
            if seconds_precision == 0:
                # a sign, [ss...]
                return 1 + leading_precision
            else:
                # a sign, [ss...].[fff...]
                return 1 + leading_precision + 1 + seconds_precision
        elif interval_range == "Day to Second":
            if seconds_precision == 0:
                # a sign, [dd...] hh:mm:ss
                return 1 + leading_precision + 9
            else:
                # a sign, [dd...] hh:mm:ss.[fff...]
                return 1 + leading_precision + 10 + seconds_precision
        elif interval_range == "Hour to Second":
            if seconds_precision == 0:
                # a sign, [hh...]:mm:ss
                return 1 + leading_precision + 6
            else:
                # a sign, [hh...]:mm:ss.[fff...]
                return 1 + leading_precision + 7 + seconds_precision
        elif interval_range == "Minute to Second":
            if seconds_precision == 0:
                # a sign, [mm...]:ss
                return 1 + leading_precision + 3
            else:
                # a sign, [mm...]:ss.[fff...]
                return 1 + leading_precision + 4 + seconds_precision
    elif data_type_oid == VerticaType.UUID:
        # aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
        return 36
    elif data_type_oid in (VerticaType.CHAR,
                           VerticaType.VARCHAR,
                           VerticaType.BINARY,
                           VerticaType.VARBINARY,
                           VerticaType.UNKNOWN):
        # the defined maximum octet length of the column
        return MAX_STRING_LEN if type_modifier <= -1 else (type_modifier - 4)
    elif data_type_oid in (VerticaType.LONGVARCHAR,
                           VerticaType.LONGVARBINARY):
        return MAX_LONG_STRING_LEN if type_modifier <= -1 else (type_modifier - 4)
    else:
        return None  # None if no meaningful values can be provided
