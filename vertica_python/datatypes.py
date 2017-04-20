

from datetime import datetime
from datetime import timedelta


# noinspection PyPep8Naming
def Date(year, month, day):
    return datetime.date(year, month, day)


# noinspection PyPep8Naming
def Time(hour, minute, second):
    return datetime.time(hour, minute, second)


# noinspection PyPep8Naming
def Timestamp(year, month, day, hour, minute, second):
    return datetime.datetime(year, month, day, hour, minute, second)


# noinspection PyPep8Naming
def DateFromTicks(ticks):
    d = datetime(1970, 1, 1) + timedelta(seconds=ticks)
    return d.date()


# noinspection PyPep8Naming
def TimeFromTicks(ticks):
    d = datetime(1970, 1, 1) + timedelta(seconds=ticks)
    return d.time()


# noinspection PyPep8Naming
def TimestampFromTicks(ticks):
    d = datetime(1970, 1, 1) + timedelta(seconds=ticks)
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
