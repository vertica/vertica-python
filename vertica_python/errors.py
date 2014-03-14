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
        super(QueryError, self).__init__("{0}, SQL: {1}".format(error_response.error_message(), repr(self.one_line_sql())))

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

QUERY_ERROR_CLASSES = {
    '55V03': type('LockFailure', (QueryError,), dict()),
    '53000': type('InsufficientResources', (QueryError,), dict()),
    '53200': type('OutOfMemory', (QueryError,), dict()),
    '42601': type('SyntaxError', (QueryError,), dict()),
    '42V01': type('MissingRelation', (QueryError,), dict()),
    '42703': type('MissingColumn', (QueryError,), dict()),
    '22V04': type('CopyRejected', (QueryError,), dict()),
    '42501': type('PermissionDenied', (QueryError,), dict()),
    '22007': type('InvalidDatetimeFormat', (QueryError,), dict())
}
