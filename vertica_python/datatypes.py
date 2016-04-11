

from datetime import datetime
from datetime import timedelta


def Date(year, month, day):
    return datetime.date(year, month, day)


def Time(hour, minute, second):
    return datetime.time(hour, minute, second)


def Timestamp(year, month, day, hour, minute, second):
    return datetime.datetime(year, month, day, hour, minute, second)


def DateFromTicks(ticks):
    d = datetime(1970, 1, 1) + timedelta(seconds=ticks)
    return d.date()


def TimeFromTicks(ticks):
    d = datetime(1970, 1, 1) + timedelta(seconds=ticks)
    return d.time()


def TimestampFromTicks(ticks):
    d = datetime(1970, 1, 1) + timedelta(seconds=ticks)
    return d.time()


class Bytea(str):
    pass


def Binary(string):
    return Bytea(string)

# vertica doesnt have a binary or row_id type i think
STRING = 9
BINARY = 10000
NUMBER = 16
DATETIME = 12
ROWID = 10001
