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

import re

from .vertica.mixins.notice_response_attr import _NoticeResponseAttrMixin


#############################################
# dbapi errors
#############################################
class Error(Exception):
    pass


# noinspection PyShadowingBuiltins
class Warning(Exception):
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

class KerberosError(ConnectionError):
    pass

class SSLNotSupported(ConnectionError):
    pass


class MessageError(InternalError):
    pass


class EmptyQueryError(ProgrammingError):
    pass


class QueryError(_NoticeResponseAttrMixin, ProgrammingError):
    """
    An error occurred associated with a query had been issued.

    This error is perhaps the most commonly encountered error type, associated
    with failures during query execution, invalid SQL statements, and more.

    Much of the details of the error are available as properties, including the
    SQL statement that had been issued (`sql`).
    """


    def __init__(self, error_response, sql):
        self.error_response = error_response
        self.sql = sql
        ProgrammingError.__init__(self,
                                  "{0}, SQL: {1}".format(error_response.error_message(),
                                                         repr(self.one_line_sql())))

    def one_line_sql(self):
        if self.sql:
            return re.sub(r"[\r\n]+", ' ', self.sql)
        else:
            return ''

    def __reduce__(self):
        # Workaround to make these exception instances (un)picklable. This must
        # be redefined by any subclass that changes the signature of __init__
        #
        # This workaround applies to python3. https://bugs.python.org/issue37489
        return (
            type(self),
            (self.error_response, self.sql),
            self.__dict__,
        )

    @property
    def _notice_attrs(self):
        # provided for _NoticeResponseAttrMixin
        if self.error_response is None:
            return {}
        return self.error_response._notice_attrs or {}

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


class MissingSchema(QueryError):
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


class DuplicateObject(QueryError):
    pass


class QueryCanceled(QueryError):
    pass


class ConnectionFailure(QueryError):
    pass


class LostConnectivityFailure(QueryError):
    pass


QUERY_ERROR_CLASSES = {
    '55V03': LockFailure,
    '53000': InsufficientResources,
    '53200': OutOfMemory,
    '42601': VerticaSyntaxError,
    '3F000': MissingSchema,
    '42V01': MissingRelation,
    '42703': MissingColumn,
    '22V04': CopyRejected,
    '42501': PermissionDenied,
    '22007': InvalidDatetimeFormat,
    '42710': DuplicateObject,
    '57014': QueryCanceled,
    '08006': ConnectionFailure,
    'V1001': LostConnectivityFailure
}
