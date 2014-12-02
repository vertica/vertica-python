from __future__ import absolute_import

import exceptions
import re


class Error(exceptions.StandardError):
    pass


class Warning(exceptions.StandardError):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


#
# Other Errors
#


class TimedOutError(OperationalError):
    pass


class ConnectionError(DatabaseError):
    pass


class SSLNotSupported(ConnectionError):
    pass


class MessageError(InternalError):
    pass


class EmptyQueryError(ProgrammingError):
    pass


class QueryError(ProgrammingError):
    def __init__(self, error_response, sql):
        self.error_response = error_response
        self.sql = sql
        super(QueryError, self).__init__("{0}, SQL: {1}".format(
            error_response.error_message(), repr(self.one_line_sql()))
        )

    def one_line_sql(self):
        if self.sql:
            return re.sub(r"[\r\n]+", ' ', self.sql)
        else:
            return ''

    @classmethod
    def from_error_response(cls, error_response, sql):
        klass = QUERY_ERROR_CLASSES.get(error_response.sqlstate, None)
        if klass is None:
            klass = cls
        return klass(error_response, sql)


class LockFailure(QueryError):
    pass


class InsufficientResources(QueryError):
    pass


class OutOfMemory(QueryError):
    pass


class VerticaSyntaxError(QueryError):
    pass


class MissingRelation(QueryError):
    pass


class MissingColumn(QueryError):
    pass


class CopyRejected(QueryError):
    pass


class PermissionDenied(QueryError):
    pass


class InvalidDatetimeFormat(QueryError):
    pass


QUERY_ERROR_CLASSES = {
    '55V03': LockFailure,
    '53000': InsufficientResources,
    '53200': OutOfMemory,
    '42601': VerticaSyntaxError,
    '42V01': MissingRelation,
    '42703': MissingColumn,
    '22V04': CopyRejected,
    '42501': PermissionDenied,
    '22007': InvalidDatetimeFormat
}
