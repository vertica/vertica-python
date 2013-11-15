from __future__ import absolute_import

import re

class VerticaError(Exception):
    pass

class ConnectionError(VerticaError):
    pass
class SSLNotSupported(ConnectionError):
    pass
class InterruptImpossible(VerticaError):
    pass
class MessageError(VerticaError):
    pass
class EmptyQueryError(VerticaError):
    pass
class TimedOutError(ConnectionError):
    pass

class SynchronizeError(VerticaError):
    def __init__(self, running_job, requested_job):
        self.running_job = running_job
        self.requested_job = requested_job
        super(SynchronizeError, self).__init__("Cannot execute {0}, connection is in use for {1}!".format(self.running_job, self.requested_job))

class QueryError(VerticaError):
    def __init__(self, error_response, sql):
        self.error_response = error_response
        self.sql = sql
        super(QueryError, self).__init__("{0}, SQL: {1}".format(error_response.error_message(), repr(self.one_line_sql())))

    def one_line_sql(self):
        return re.sub(r"[\r\n]+", ' ', self.sql)

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
