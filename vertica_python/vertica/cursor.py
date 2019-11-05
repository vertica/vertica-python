# Copyright (c) 2018-2019 Micro Focus or one of its affiliates.
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

import datetime
import inspect
import re
from io import IOBase
from tempfile import NamedTemporaryFile, SpooledTemporaryFile, TemporaryFile
from uuid import UUID
from collections import OrderedDict

# _TemporaryFileWrapper is an undocumented implementation detail, so
# import defensively.
try:
    from tempfile import _TemporaryFileWrapper
except ImportError:
    _TemporaryFileWrapper = None

import six
# noinspection PyUnresolvedReferences,PyCompatibility
from builtins import str
from six import binary_type, text_type, string_types, BytesIO, StringIO

from .. import errors
from ..compat import as_text
from ..vertica import messages
from ..vertica.column import Column


# A note regarding support for temporary files:
#
# Since Python 2.6, the tempfile module offers three kinds of temporary
# files:
#
#   * NamedTemporaryFile
#   * SpooledTemporaryFile
#   * TemporaryFile
#
# NamedTemporaryFile is not a class, but a function that returns
# an instance of the tempfile._TemporaryFileWrapper class.
# _TemporaryFileWrapper is a direct subclass of object.
#
#   * https://github.com/python/cpython/blob/v3.8.0/Lib/tempfile.py#L546
#   * https://github.com/python/cpython/blob/v3.8.0/Lib/tempfile.py#L450
#
# SpooledTemporaryFile is a class that is a direct subclass of object.
#
#   * https://github.com/python/cpython/blob/v3.8.0/Lib/tempfile.py#L623
#
# TemporaryFile is a class that is either NamedTemporaryFile or an
# indirect subclass of io.IOBase, depending on the platform.
#
#   * https://bugs.python.org/issue33762
#   * https://github.com/python/cpython/blob/v3.8.0/Lib/tempfile.py#L552-L555
#   * https://github.com/python/cpython/blob/v3.8.0/Lib/tempfile.py#L606-L608
#   * https://github.com/python/cpython/blob/v3.8.0/Lib/tempfile.py#L617-L618
#
# As a result, for Python 2.6 and newer, it seems the best way to test
# for a file-like object inclusive of temporary files is via:
#
#   isinstance(obj, (IOBase, SpooledTemporaryFile, _TemporaryFileWrapper))

# Of the following "types", only include those that are classes in
# file_type so that isinstance(obj, file_type) won't fail. As of Python
# 3.8 only IOBase, SpooledTemporaryFile and _TemporaryFileWrapper are
# classes, but if future Python versions implement NamedTemporaryFile
# and TemporaryFile as classes, the following code should account for
# that accordingly.
file_type = tuple(
    type_ for type_ in [
        IOBase,
        NamedTemporaryFile,
        SpooledTemporaryFile,
        TemporaryFile,
        _TemporaryFileWrapper,
    ]
    if inspect.isclass(type_)
)
if six.PY2:
    # noinspection PyUnresolvedReferences
    file_type = file_type + (file,)

NULL = "NULL"

RE_NAME_BASE = u"[a-zA-Z_][\\w\\d\\$_]*"
RE_NAME = u'(("{0}")|({0}))'.format(RE_NAME_BASE)
RE_BASIC_INSERT_STAT = (
    u"INSERT\\s+INTO\\s+(?P<target>({0}\\.)?{0})"
    u"\\s*\\(\\s*(?P<variables>{0}(\\s*,\\s*{0})*)\\s*\\)"
    u"\\s+VALUES\\s*\\(\\s*(?P<values>(.|\\s)*)\\s*\\)").format(RE_NAME)
END_OF_RESULT_RESPONSES = (messages.CommandComplete, messages.PortalSuspended)


class Cursor(object):
    # NOTE: this is used in executemany and is here for pandas compatibility
    _insert_statement = re.compile(RE_BASIC_INSERT_STAT, re.U | re.I)

    def __init__(self, connection, logger, cursor_type=None, unicode_error=None):
        self.connection = connection
        self._logger = logger
        self.cursor_type = cursor_type
        self.unicode_error = unicode_error if unicode_error is not None else 'strict'
        self._closed = False
        self._message = None
        self.operation = None
        self.prepared_sql = None  # last statement been prepared
        self.prepared_name = "s0"
        self.error = None

        #
        # dbapi attributes
        #
        self.description = None
        self.rowcount = -1
        self.arraysize = 1

    #############################################
    # supporting `with` statements
    #############################################
    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.close()

    #############################################
    # dbapi methods
    #############################################
    # noinspection PyMethodMayBeStatic
    def callproc(self, procname, parameters=None):
        raise errors.NotSupportedError('Cursor.callproc() is not implemented')

    def close(self):
        self._logger.info('Close the cursor')
        if not self.closed() and self.prepared_sql:
            self._close_prepared_statement()
        self._closed = True

    def execute(self, operation, parameters=None, use_prepared_statements=None):
        operation = as_text(operation)
        self.operation = operation

        if self.closed():
            raise errors.InterfaceError('Cursor is closed')

        self.flush_to_query_ready()

        self.rowcount = -1

        use_prepared = bool(self.connection.options['use_prepared_statements']
                if use_prepared_statements is None else use_prepared_statements)
        if use_prepared:
            # Execute the SQL as prepared statement (server-side bindings)
            if parameters and not isinstance(parameters, (list, tuple)):
                raise TypeError("Execute parameters should be a list/tuple")

            # If the SQL has not been prepared, prepare the SQL
            if operation != self.prepared_sql:
                self._prepare(operation)
                self.prepared_sql = operation  # the prepared statement is kept

            # Bind the parameters and execute
            self._execute_prepared_statement([parameters])
        else:
            # Execute the SQL directly (client-side bindings)
            if parameters:
                operation = self.format_operation_with_parameters(operation, parameters)
            self._execute_simple_query(operation)

        return self

    def executemany(self, operation, seq_of_parameters, use_prepared_statements=None):
        operation = as_text(operation)
        self.operation = operation

        if not isinstance(seq_of_parameters, (list, tuple)):
            raise TypeError("seq_of_parameters should be list/tuple")

        if self.closed():
            raise errors.InterfaceError('Cursor is closed')

        self.flush_to_query_ready()
        use_prepared = bool(self.connection.options['use_prepared_statements']
                if use_prepared_statements is None else use_prepared_statements)

        if use_prepared:
            # Execute the SQL as prepared statement (server-side bindings)
            if len(seq_of_parameters) == 0:
                raise ValueError("seq_of_parameters should not be empty")
            if not all(isinstance(elem, (list, tuple)) for elem in seq_of_parameters):
                raise TypeError("Each seq_of_parameters element should be a list/tuple")
            # If the SQL has not been prepared, prepare the SQL
            if operation != self.prepared_sql:
                self._prepare(operation)
                self.prepared_sql = operation  # the prepared statement is kept

            # Bind the parameters and execute
            self._execute_prepared_statement(seq_of_parameters)
        else:
            m = self._insert_statement.match(operation)
            if m:
                target = as_text(m.group('target'))

                variables = as_text(m.group('variables'))
                variables = ",".join([variable.strip().strip('"') for variable in variables.split(",")])

                values = as_text(m.group('values'))
                values = ",".join([value.strip().strip('"') for value in values.split(",")])
                seq_of_values = [self.format_operation_with_parameters(values, parameters, is_csv=True)
                                 for parameters in seq_of_parameters]
                data = "\n".join(seq_of_values)

                copy_autocommit = self.connection.parameters.get('auto_commit', 'on')

                copy_statement = (
                    u"COPY {0} ({1}) FROM STDIN DELIMITER ',' ENCLOSED BY '\"' "
                    u"ENFORCELENGTH ABORT ON ERROR{2}").format(target, variables,
                    " NO COMMIT" if copy_autocommit == 'off' else '')

                self.copy(copy_statement, data)
            else:
                raise NotImplementedError(
                    "executemany is implemented for simple INSERT statements only")

    def fetchone(self):
        while True:
            if isinstance(self._message, messages.DataRow):
                if self.rowcount == -1:
                    self.rowcount = 1
                else:
                    self.rowcount += 1
                row = self.row_formatter(self._message)
                # fetch next message
                self._message = self.connection.read_message()
                return row
            elif isinstance(self._message, messages.RowDescription):
                self.description = [Column(fd, self.unicode_error) for fd in self._message.fields]
            elif isinstance(self._message, messages.ReadyForQuery):
                return None
            elif isinstance(self._message, END_OF_RESULT_RESPONSES):
                return None
            elif isinstance(self._message, messages.EmptyQueryResponse):
                pass
            elif isinstance(self._message, messages.ErrorResponse):
                raise errors.QueryError.from_error_response(self._message, self.operation)
            else:
                raise errors.MessageError('Unexpected fetchone() state: {}'.format(
                                    type(self._message).__name__))

            self._message = self.connection.read_message()

    def fetchmany(self, size=None):
        if not size:
            size = self.arraysize
        results = []
        while True:
            row = self.fetchone()
            if not row:
                break
            results.append(row)
            if len(results) >= size:
                break
        return results

    def fetchall(self):
        return list(self.iterate())

    def nextset(self):
        """
        Skip to the next available result set, discarding any remaining rows
        from the current result set.

        If there are no more result sets, this method returns False. Otherwise,
        it returns a True and subsequent calls to the fetch*() methods will
        return rows from the next result set.
        """
        # skip any data for this set if exists
        self.flush_to_end_of_result()

        if self._message is None:
            return False
        elif isinstance(self._message, END_OF_RESULT_RESPONSES):
            # there might be another set, read next message to find out
            self._message = self.connection.read_message()
            if isinstance(self._message, messages.RowDescription):
                self.description = [Column(fd, self.unicode_error) for fd in self._message.fields]
                self._message = self.connection.read_message()
                return True
            elif isinstance(self._message, messages.BindComplete):
                self._message = self.connection.read_message()
                return True
            elif isinstance(self._message, messages.ReadyForQuery):
                return False
            elif isinstance(self._message, END_OF_RESULT_RESPONSES):
                # result of a DDL/transaction
                return True
            elif isinstance(self._message, messages.ErrorResponse):
                raise errors.QueryError.from_error_response(self._message, self.operation)
            else:
                raise errors.MessageError(
                    'Unexpected nextset() state after END_OF_RESULT_RESPONSES: {0}'.format(self._message))
        elif isinstance(self._message, messages.ReadyForQuery):
            # no more sets left to be read
            return False
        else:
            raise errors.MessageError('Unexpected nextset() state: {0}'.format(self._message))

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    #############################################
    # non-dbapi methods
    #############################################
    def closed(self):
        return self._closed or self.connection.closed()

    def cancel(self):
        # Cancel is a session-level operation, cursor-level API does not make
        # sense. Keep this API for backward compatibility.
        raise errors.NotSupportedError(
            'Cursor.cancel() is deprecated. Call Connection.cancel() '
            'to cancel the current database operation.')

    def iterate(self):
        row = self.fetchone()
        while row:
            yield row
            row = self.fetchone()

    def copy(self, sql, data, **kwargs):
        """

        EXAMPLE:
        >> with open("/tmp/file.csv", "rb") as fs:
        >>     cursor.copy("COPY table(field1,field2) FROM STDIN DELIMITER ',' ENCLOSED BY ''''",
        >>                 fs, buffer_size=65536)

        """
        sql = as_text(sql)

        if self.closed():
            raise errors.InterfaceError('Cursor is closed')

        self.flush_to_query_ready()

        if isinstance(data, binary_type):
            stream = BytesIO(data)
        elif isinstance(data, text_type):
            stream = StringIO(data)
        elif isinstance(data, file_type):
            stream = data
        else:
            raise TypeError("Not valid type of data {0}".format(type(data)))

        self._logger.info(u'Execute COPY statement: [{}]'.format(sql))
        self.connection.write(messages.Query(sql))

        while True:
            message = self.connection.read_message()

            self._message = message
            if isinstance(message, messages.ErrorResponse):
                raise errors.QueryError.from_error_response(message, sql)
            elif isinstance(message, messages.ReadyForQuery):
                break
            elif isinstance(message, messages.CopyInResponse):
                self.connection.write(messages.CopyStream(stream, **kwargs))
                self.connection.write(messages.CopyDone())
            elif isinstance(message, messages.CommandComplete):
                pass
            else:
                raise errors.MessageError('Unexpected message: {0}'.format(message))

        if self.error is not None:
            raise self.error

    #############################################
    # internal
    #############################################
    def flush_to_query_ready(self):
        # if the last message isn't empty or ReadyForQuery, read all remaining messages
        if self._message is None \
                or isinstance(self._message, messages.ReadyForQuery):
            return

        while True:
            message = self.connection.read_message()
            if isinstance(message, messages.ReadyForQuery):
                self._message = message
                break

    def flush_to_end_of_result(self):
        # if the last message isn't empty or END_OF_RESULT_RESPONSES,
        # read messages until it is
        if (self._message is None or
            isinstance(self._message, messages.ReadyForQuery) or
            isinstance(self._message, END_OF_RESULT_RESPONSES)):
            return

        while True:
            message = self.connection.read_message()
            if isinstance(message, END_OF_RESULT_RESPONSES):
                self._message = message
                break

    def row_formatter(self, row_data):
        if self.cursor_type is None:
            return self.format_row_as_array(row_data)
        elif self.cursor_type in (list, 'list'):
            return self.format_row_as_array(row_data)
        elif self.cursor_type in (dict, 'dict'):
            return self.format_row_as_dict(row_data)
        else:
            raise TypeError('Unrecognized cursor_type: {0}'.format(self.cursor_type))

    def format_row_as_dict(self, row_data):
        return OrderedDict(
            (self.description[idx].name, self.description[idx].convert(value))
            for idx, value in enumerate(row_data.values)
        )

    def format_row_as_array(self, row_data):
        return [self.description[idx].convert(value)
                for idx, value in enumerate(row_data.values)]

    # noinspection PyArgumentList
    def format_operation_with_parameters(self, operation, parameters, is_csv=False):
        operation = as_text(operation)

        if isinstance(parameters, dict):
            for key, param in six.iteritems(parameters):
                if not isinstance(key, string_types):
                    key = str(key)
                key = as_text(key)

                if isinstance(param, (string_types, bytes)):
                    param = self.format_quote(as_text(param), is_csv)
                elif isinstance(param, (datetime.datetime, datetime.date, datetime.time, UUID)):
                    param = self.format_quote(as_text(str(param)), is_csv)
                elif param is None:
                    param = '' if is_csv else NULL
                else:
                    param = str(param)
                value = as_text(param)

                # Using a regex with word boundary to correctly handle params with similar names
                # such as :s and :start
                match_str = u":{0}\\b".format(key)
                operation = re.sub(match_str, value, operation, flags=re.U)

        elif isinstance(parameters, (tuple, list)):
            tlist = []
            for param in parameters:
                if isinstance(param, (string_types, bytes)):
                    param = self.format_quote(as_text(param), is_csv)
                elif isinstance(param, (datetime.datetime, datetime.date, datetime.time, UUID)):
                    param = self.format_quote(as_text(str(param)), is_csv)
                elif param is None:
                    param = '' if is_csv else NULL
                else:
                    param = str(param)
                value = as_text(param)

                tlist.append(value)

            operation = operation % tuple(tlist)
        else:
            raise TypeError("Argument 'parameters' must be dict or tuple/list")

        return operation

    def format_quote(self, param, is_csv):
        if is_csv:
            return u'"{0}"'.format(re.escape(param))
        else:
            return u"'{0}'".format(param.replace(u"'", u"''").replace(u"\\", u"\\\\"))

    def _execute_simple_query(self, query):
        """
        Send the query to the server using the simple query protocol.
        Return True if this query contained no SQL (e.g. the string "--comment")
        """
        self._logger.info(u'Execute simple query: [{}]'.format(query))

        # All of the statements in the query are sent here in a single message
        self.connection.write(messages.Query(query))

        # The first response could be a number of things:
        #   ErrorResponse: Something went wrong on the server.
        #   EmptyQueryResponse: The query being executed is empty.
        #   RowDescription: This is the "normal" case when executing a query.
        #                   It marks the start of the results.
        #   CommandComplete: This occurs when executing DDL/transactions.
        self._message = self.connection.read_message()
        if isinstance(self._message, messages.ErrorResponse):
            raise errors.QueryError.from_error_response(self._message, query)
        elif isinstance(self._message, messages.RowDescription):
            self.description = [Column(fd, self.unicode_error) for fd in self._message.fields]
            self._message = self.connection.read_message()
            if isinstance(self._message, messages.ErrorResponse):
                raise errors.QueryError.from_error_response(self._message, query)

    def _error_handler(self, msg):
        self.connection.write(messages.Sync())
        raise errors.QueryError.from_error_response(msg, self.operation)

    def _prepare(self, query):
        """
        Send the query to be prepared to the server. The server will parse the
        query and return some metadata.
        """
        self._logger.info(u'Prepare a statement: [{}]'.format(query))

        # Send Parse message to server
        # We don't need to tell the server the parameter types yet
        self.connection.write(messages.Parse(self.prepared_name, query, param_types=()))
        # Send Describe message to server
        self.connection.write(messages.Describe('prepared_statement', self.prepared_name))
        self.connection.write(messages.Flush())

        # Read expected message: ParseComplete
        self._message = self.connection.read_expected_message(messages.ParseComplete, self._error_handler)

        # Read expected message: ParameterDescription
        self._message = self.connection.read_expected_message(messages.ParameterDescription, self._error_handler)
        self._param_metadata = self._message.parameters

        # Read expected message: RowDescription or NoData
        self._message = self.connection.read_expected_message(
                        (messages.RowDescription, messages.NoData), self._error_handler)
        if isinstance(self._message, messages.NoData):
            self.description = None  # response was NoData for a DDL/transaction PreparedStatement
        else:
            self.description = [Column(fd, self.unicode_error) for fd in self._message.fields]

        # Read expected message: CommandDescription
        self._message = self.connection.read_expected_message(messages.CommandDescription, self._error_handler)
        if len(self._message.command_tag) == 0:
            msg = 'The statement being prepared is empty'
            self._logger.error(msg)
            self.connection.write(messages.Sync())
            raise errors.EmptyQueryError(msg)

        self._logger.info('Finish preparing the statement')

    def _execute_prepared_statement(self, list_of_parameter_values):
        """
        Send multiple statement parameter sets to the server using the extended
        query protocol. The server would bind and execute each set of parameter
        values.

        This function should not be called without first calling _prepare() to
        prepare a statement.
        """
        portal_name = ""
        parameter_type_oids = [metadata['data_type_oid'] for metadata in self._param_metadata]
        parameter_count = len(self._param_metadata)

        try:
            if len(list_of_parameter_values) == 0:
                raise ValueError("Empty list/tuple, nothing to execute")
            for parameter_values in list_of_parameter_values:
                if parameter_values is None:
                    parameter_values = ()
                self._logger.info(u'Bind parameters: {}'.format(parameter_values))
                if len(parameter_values) != parameter_count:
                    msg = ("Invalid number of parameters for {}: {} given, {} expected"
                           .format(parameter_values, len(parameter_values), parameter_count))
                    raise ValueError(msg)
                self.connection.write(messages.Bind(portal_name, self.prepared_name,
                                             parameter_values, parameter_type_oids))
                self.connection.write(messages.Execute(portal_name, 0))
            self.connection.write(messages.Sync())
        except Exception as e:
            self._logger.error(str(e))
            # the server will not send anything until we issue a sync
            self.connection.write(messages.Sync())
            self._message = self.connection.read_message()
            raise

        self.connection.write(messages.Flush())

        # Read expected message: BindComplete
        self.connection.read_expected_message(messages.BindComplete)

        self._message = self.connection.read_message()
        if isinstance(self._message, messages.ErrorResponse):
            raise errors.QueryError.from_error_response(self._message, self.prepared_sql)

    def _close_prepared_statement(self):
        """
        Close the prepared statement on the server.
        """
        self.prepared_sql = None
        self.flush_to_query_ready()
        self.connection.write(messages.Close('prepared_statement', self.prepared_name))
        self.connection.write(messages.Flush())
        self._message = self.connection.read_expected_message(messages.CloseComplete)
        self.connection.write(messages.Sync())
