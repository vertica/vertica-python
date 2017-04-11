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


# vertica doesnt have a binary or row_id type i think
STRING = 9
BINARY = 10000
NUMBER = 16
DATETIME = 12
ROWID = 10001
