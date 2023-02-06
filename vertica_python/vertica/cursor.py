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

import datetime
import glob
import inspect
import re
import sys
import traceback
from decimal import Decimal
from io import IOBase, BytesIO, StringIO
from tempfile import NamedTemporaryFile, SpooledTemporaryFile, TemporaryFile
from uuid import UUID
from collections import OrderedDict

# _TemporaryFileWrapper is an undocumented implementation detail, so
# import defensively.
try:
    from tempfile import _TemporaryFileWrapper
except ImportError:
    _TemporaryFileWrapper = None

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from typing import IO, Any, AnyStr, Callable, Dict, Generator, List, Literal, Optional, Sequence, Tuple, Type, TypeVar, Union
    from typing_extensions import Self
    from .connection import Connection
    from logging import Logger
    T = TypeVar('T')

from .. import errors, os_utils
from ..compat import as_text
from ..vertica import messages
from ..vertica.column import Column
from ..vertica.deserializer import Deserializer


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


RE_NAME_BASE = u"[0-9a-zA-Z_][\\w\\d\\$_]*"
RE_NAME = u'(("{0}")|({0}))'.format(RE_NAME_BASE)
RE_BASIC_INSERT_STAT = (
    u"\\s*INSERT\\s+INTO\\s+(?P<target>({0}\\.)?{0})"
    u"\\s*\\(\\s*(?P<variables>{0}(\\s*,\\s*{0})*)\\s*\\)"
    u"\\s+VALUES\\s*\\(\\s*(?P<values>(.|\\s)*)\\s*\\)").format(RE_NAME)
END_OF_RESULT_RESPONSES = (messages.CommandComplete, messages.PortalSuspended)
END_OF_BATCH_RESPONSES = (messages.WriteFile, messages.EndOfBatchResponse)
DEFAULT_BUFFER_SIZE = 131072


class Cursor(object):
    # NOTE: this is used in executemany and is here for pandas compatibility
    _insert_statement = re.compile(RE_BASIC_INSERT_STAT, re.U | re.I)

    def __init__(self, connection, logger, cursor_type=None, unicode_error=None):
        # type: (Connection, Logger, Optional[Union[Literal['list', 'dict'], Type[list[Any]], Type[dict[Any, Any]]]], Optional[str]) -> None
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
        self._sql_literal_adapters = {}
        self._disable_sqldata_converter = False
        self._des = Deserializer()

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
        # type: () -> Self
        return self

    def __exit__(self, type_, value, traceback):
        self.close()

    #############################################
    # decorators
    #############################################
    def handle_ctrl_c(func):
        """
        On Ctrl-C, try to cancel the query in the server
        """
        def wrap(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except KeyboardInterrupt:
                self.connection.cancel()
                self.flush_to_query_ready() # ignore errors.QueryCanceled
                raise
        return wrap

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

    @handle_ctrl_c
    def execute(self, operation, parameters=None, use_prepared_statements=None,
                copy_stdin=None, buffer_size=DEFAULT_BUFFER_SIZE):
        # type: (str, Optional[Union[List[Any], Tuple[Any], Dict[str, Any]]], Optional[bool], Optional[Union[IO[AnyStr], List[IO[AnyStr]]]], int) -> Self
        if self.closed():
            raise errors.InterfaceError('Cursor is closed')

        self.flush_to_query_ready()

        operation = as_text(operation)
        self.operation = operation

        self.rowcount = -1

        if copy_stdin is None:
            self.copy_stdin_list = []
        elif (isinstance(copy_stdin, list) and
              all(callable(getattr(i, 'read', None)) for i in copy_stdin)):
            self.copy_stdin_list = copy_stdin
        elif callable(getattr(copy_stdin, 'read', None)):
            self.copy_stdin_list = [copy_stdin]
        else:
            raise TypeError("Cursor.execute 'copy_stdin' parameter should be"
                            " a file-like object or a list of file-like objects")
        self.buffer_size = buffer_size   # For copy-local read and write

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

    @handle_ctrl_c
    def executemany(self, operation, seq_of_parameters, use_prepared_statements=None):
        # type: (str, Sequence[Union[List[Any], Tuple[Any], Dict[str, Any]]], Optional[bool]) -> None

        if not isinstance(seq_of_parameters, (list, tuple)):
            raise TypeError("seq_of_parameters should be list/tuple")

        if self.closed():
            raise errors.InterfaceError('Cursor is closed')

        self.flush_to_query_ready()

        operation = as_text(operation)
        self.operation = operation

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
                seq_of_values = [self.format_operation_with_parameters(values, parameters, is_copy_data=True)
                                 for parameters in seq_of_parameters]
                data = "\n".join(seq_of_values)

                copy_statement = (
                    u"COPY {0} ({1}) FROM STDIN DELIMITER ',' ENCLOSED BY '\"' "
                    u"ENFORCELENGTH ABORT ON ERROR{2}").format(target, variables,
                    " NO COMMIT" if not self.connection.autocommit else '')

                self.copy(copy_statement, data)
            else:
                raise NotImplementedError(
                    "executemany is implemented for simple INSERT statements only")

    def fetchone(self):
        # type: () -> Optional[Union[List[Any], OrderedDict[str, Any]]]
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
                self.description = self._message.get_description()
                self._deserializers = self._des.get_row_deserializers(self.description,
                                        {'unicode_error': self.unicode_error,
                                         'session_tz': self.connection.parameters.get('timezone', 'unknown'),
                                         'complex_types_enabled': self.connection.complex_types_enabled,})
            elif isinstance(self._message, messages.ReadyForQuery):
                return None
            elif isinstance(self._message, END_OF_RESULT_RESPONSES):
                return None
            elif isinstance(self._message, messages.EmptyQueryResponse):
                pass
            elif isinstance(self._message, messages.VerifyFiles):
                self._handle_copy_local_protocol()
            elif isinstance(self._message, messages.EndOfBatchResponse):
                pass
            elif isinstance(self._message, messages.CopyDoneResponse):
                pass
            elif isinstance(self._message, messages.ErrorResponse):
                raise errors.QueryError.from_error_response(self._message, self.operation)
            else:
                raise errors.MessageError('Unexpected fetchone() state: {}'.format(
                                    type(self._message).__name__))

            self._message = self.connection.read_message()

    def fetchmany(self, size=None):
        # type: (Optional[int]) -> List[Union[List[Any], OrderedDict[str, Any]]]
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
        # type: () -> bool
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
                self.description = self._message.get_description()
                self._deserializers = self._des.get_row_deserializers(self.description,
                                        {'unicode_error': self.unicode_error,
                                         'session_tz': self.connection.parameters.get('timezone', 'unknown'),
                                         'complex_types_enabled': self.connection.complex_types_enabled,})
                self._message = self.connection.read_message()
                if isinstance(self._message, messages.VerifyFiles):
                    self._handle_copy_local_protocol()
                self.rowcount = -1
                return True
            elif isinstance(self._message, messages.BindComplete):
                self._message = self.connection.read_message()
                self.rowcount = -1
                return True
            elif isinstance(self._message, messages.ReadyForQuery):
                return False
            elif isinstance(self._message, END_OF_RESULT_RESPONSES):
                # result of a DDL/transaction
                self.rowcount = -1
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
        # type: () -> bool
        return self._closed or self.connection.closed()

    def cancel(self):
        # Cancel is a session-level operation, cursor-level API does not make
        # sense. Keep this API for backward compatibility.
        raise errors.NotSupportedError(
            'Cursor.cancel() is deprecated. Call Connection.cancel() '
            'to cancel the current database operation.')

    def iterate(self):
        # type: () -> Generator[Union[List[Any], OrderedDict[str, Any]], None, None]
        row = self.fetchone()
        while row:
            yield row
            row = self.fetchone()

    def copy(self, sql, data, **kwargs):
        # type: (str, IO[AnyStr], Any) -> None
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

        if isinstance(data, bytes):
            stream = BytesIO(data)
        elif isinstance(data, str):
            stream = StringIO(data)
        elif isinstance(data, file_type) or callable(getattr(data, 'read', None)):
            stream = data
        else:
            raise TypeError("Not valid type of data {0}".format(type(data)))

        # TODO: check sql is a valid `COPY FROM STDIN` SQL statement

        self._logger.info(u'Execute COPY statement: [{}]'.format(sql))
        # Execute a `COPY FROM STDIN` SQL statement
        self.connection.write(messages.Query(sql))

        buffer_size = kwargs['buffer_size'] if 'buffer_size' in kwargs else DEFAULT_BUFFER_SIZE

        while True:
            message = self.connection.read_message()

            self._message = message
            if isinstance(message, messages.ErrorResponse):
                raise errors.QueryError.from_error_response(message, sql)
            elif isinstance(message, messages.ReadyForQuery):
                break
            elif isinstance(message, messages.CommandComplete):
                pass
            elif isinstance(message, messages.CopyInResponse):
                try:
                    self._send_copy_data(stream, buffer_size)
                except Exception as e:
                    # COPY termination: report the cause of failure to the backend
                    self.connection.write(messages.CopyFail(str(e)))
                    self._logger.error(str(e))
                    self.flush_to_query_ready()
                    raise errors.DataError('Failed to send a COPY data stream: {}'.format(str(e)))

                # Successful termination for COPY
                self.connection.write(messages.CopyDone())
            else:
                raise errors.MessageError('Unexpected message: {0}'.format(message))

        if self.error is not None:
            raise self.error

    def object_to_sql_literal(self, py_obj):
        return self.object_to_string(py_obj, False)

    def register_sql_literal_adapter(self, obj_type, adapter_func):
        # type: (T, Callable[[T], str]) -> None
        if not callable(adapter_func):
            raise TypeError("Cannot register this sql literal adapter. The adapter is not callable.")
        self._sql_literal_adapters[obj_type] = adapter_func

    @property
    def disable_sqldata_converter(self):
        return self._disable_sqldata_converter

    @disable_sqldata_converter.setter
    def disable_sqldata_converter(self, value):
        """By default, the client does data conversions for query results:
        reading a bytes sequence from server and creating a Python object out of it.
        If set to True, bypass conversions from SQL type raw data to the native Python object
        """
        self._disable_sqldata_converter = bool(value)

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
            elif isinstance(message, messages.VerifyFiles):
                self._message = message
                self._handle_copy_local_protocol()

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
        if self._disable_sqldata_converter:
            return OrderedDict((descr.name, value)
                    for descr, value in zip(self.description, row_data.values))
        return OrderedDict(
            (descr.name, convert(value))
            for descr, convert, value in zip(self.description, self._deserializers, row_data.values)
        )

    def format_row_as_array(self, row_data):
        if self._disable_sqldata_converter:
            return row_data.values
        return [convert(value)
                for convert, value in zip(self._deserializers, row_data.values)]

    def object_to_string(self, py_obj, is_copy_data):
        """Return the SQL representation of the object as a string"""
        if type(py_obj) in self._sql_literal_adapters and not is_copy_data:
            adapter = self._sql_literal_adapters[type(py_obj)]
            result = adapter(py_obj)
            if not isinstance(result, (str, bytes)):
                raise TypeError("Unexpected return type of {} adapter: {}, expected a string type."
                    .format(type(py_obj), type(result)))
            return as_text(result)

        if isinstance(py_obj, type(None)):
            return '' if is_copy_data else 'NULL'
        elif isinstance(py_obj, bool):
            return str(py_obj)
        elif isinstance(py_obj, (str, bytes)):
            return self.format_quote(as_text(py_obj), is_copy_data)
        elif isinstance(py_obj, (int, float, Decimal)):
            return str(py_obj)
        elif isinstance(py_obj, tuple):  # tuple and namedtuple
            elements = [None] * len(py_obj)
            for i in range(len(py_obj)):
                elements[i] = self.object_to_string(py_obj[i], is_copy_data)
            return "(" + ",".join(elements) + ")"
        elif isinstance(py_obj, (datetime.datetime, datetime.date, datetime.time, UUID)):
            return self.format_quote(as_text(str(py_obj)), is_copy_data)
        else:
            if is_copy_data:
                return str(py_obj)
            else:
                msg = ("Cannot convert {} type object to an SQL string. "
                       "Please register a new adapter for this type via the "
                       "Cursor.register_sql_literal_adapter() function."
                       .format(type(py_obj)))
                raise TypeError(msg)

    # noinspection PyArgumentList
    def format_operation_with_parameters(self, operation, parameters, is_copy_data=False):
        operation = as_text(operation)

        if isinstance(parameters, dict):
            for key, param in parameters.items():
                if not isinstance(key, str):
                    key = str(key)
                key = as_text(key)

                value = self.object_to_string(param, is_copy_data)

                # Using a regex with word boundary to correctly handle params with similar names
                # such as :s and :start
                match_str = u":{0}\\b".format(key)
                operation = re.sub(match_str, lambda _: value, operation, flags=re.U)

        elif isinstance(parameters, (tuple, list)):
            tlist = []
            for param in parameters:
                value = self.object_to_string(param, is_copy_data)
                tlist.append(value)

            operation = operation % tuple(tlist)
        else:
            raise TypeError("Argument 'parameters' must be dict or tuple/list")

        return operation

    def format_quote(self, param, is_copy_data):
        if is_copy_data:
            s = list(param)
            for i, c in enumerate(param):
                if c in u'()[]{}?"*+-|^$\\.&~# \t\n\r\v\f':
                    s[i] = "\\" + c
            return u'"{0}"'.format(u"".join(s))
        else:
            return u"'{0}'".format(param.replace(u"'", u"''"))

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
            self.description = self._message.get_description()
            self._deserializers = self._des.get_row_deserializers(self.description,
                                    {'unicode_error': self.unicode_error,
                                     'session_tz': self.connection.parameters.get('timezone', 'unknown'),
                                     'complex_types_enabled': self.connection.complex_types_enabled,})
            self._message = self.connection.read_message()
            if isinstance(self._message, messages.ErrorResponse):
                raise errors.QueryError.from_error_response(self._message, query)
            elif isinstance(self._message, messages.VerifyFiles):
                self._handle_copy_local_protocol()

    def _handle_copy_local_protocol(self):
        if self.connection.options['disable_copy_local']:
            msg = 'COPY LOCAL operation is disabled.'
            self.connection.write(messages.CopyError(msg))
            self.flush_to_query_ready()
            raise errors.InterfaceError(msg)

        # Extract info from VerifyFiles message
        input_files = self._message.input_files
        rejections_file = self._message.rejections_file
        exceptions_file = self._message.exceptions_file

        # Verify the file(s) present in the COPY FROM LOCAL statement are indeed accessible
        self.valid_write_file_path = []
        try:
            # Check that the output files are writable
            if rejections_file != '':
                if rejections_file not in self.operation:
                    raise errors.MessageError('Server requests for writing to'
                        ' invalid rejected file path: {}'.format(rejections_file))
                os_utils.check_file_writable(rejections_file)
                self.valid_write_file_path.append(rejections_file)
            if exceptions_file != '':
                if exceptions_file not in self.operation:
                    raise errors.MessageError('Server requests for writing to'
                        ' invalid exceptions file path: {}'.format(exceptions_file))
                os_utils.check_file_writable(exceptions_file)
                self.valid_write_file_path.append(exceptions_file)

            # Check that the input files are readable
            self.valid_read_file_path = self._check_copy_local_files(input_files)

            self.connection.write(messages.VerifiedFiles(self.valid_read_file_path))
        except Exception as e:
            tb = sys.exc_info()[2]
            stk = traceback.extract_tb(tb, 1)
            self.connection.write(messages.CopyError(str(e), stk[0]))
            self.flush_to_query_ready()
            raise

        # Server should be ready to receive copy data from STDIN/files
        self._message = self.connection.read_message()
        if isinstance(self._message, messages.ErrorResponse):
            raise errors.QueryError.from_error_response(self._message, self.operation)
        elif not isinstance(self._message, (messages.CopyInResponse, messages.LoadFile)):
            raise errors.MessageError('Unexpected COPY FROM LOCAL state: {}'.format(
                                      type(self._message).__name__))
        try:
            if isinstance(self._message, messages.CopyInResponse):
                self._logger.info('Sending STDIN data to server')
                if len(self.copy_stdin_list) == 0:
                    raise ValueError('No STDIN source to load. Please specify "copy_stdin" parameter in Cursor.execute()')
                stdin = self.copy_stdin_list.pop(0)
                self._send_copy_data(stdin, self.buffer_size)
                self.connection.write(messages.EndOfBatchRequest())
                self._read_copy_data_response(is_stdin_copy=True)
            elif isinstance(self._message, messages.LoadFile):
                while True:
                    self._send_copy_file_data()
                    if not self._read_copy_data_response():
                        break
        except errors.QueryError:
            # A server-detected error.
            # The server issues an ErrorResponse message and a ReadyForQuery message.
            raise
        except Exception as e:
            # A client-detected error.
            # The client terminates COPY LOCAL protocol by sending a CopyError message,
            # which will cause the COPY SQL statement to fail with an ErrorResponse message.
            tb = sys.exc_info()[2]
            stk = traceback.extract_tb(tb, 1)
            self.connection.write(messages.CopyError(str(e), stk[0]))
            self.flush_to_query_ready()
            raise

    def _check_copy_local_files(self, input_files):
        # Return an empty list when the copy input is STDIN
        if len(input_files) == 0:
            return []

        file_list = []
        for file_pattern in input_files:
            if file_pattern not in self.operation:
                raise errors.MessageError('Server requests for loading invalid'
                    ' file: {}, Query: {}'.format(file_pattern, self.operation))
            # Expand the glob patterns
            expanded_files = glob.glob(file_pattern)
            if len(expanded_files) == 0:
                raise OSError('{} does not exist'.format(file_pattern))
            # Check file permissions
            for f in expanded_files:
                os_utils.check_file_readable(f)
                file_list.append(f)
        # Return a non-empty list when the copy input is FILE
        # Note: Sending an empty list of files will make server kill the session.
        return file_list

    def _send_copy_data(self, stream, buffer_size):
        # Send zero or more CopyData messages, forming a stream of input data
        while True:
            chunk = stream.read(buffer_size)
            if not chunk:
                break
            self.connection.write(messages.CopyData(chunk, self.unicode_error))

    def _send_copy_file_data(self):
        filename = self._message.filename
        self._logger.info('Sending {} data to server'.format(filename))

        if filename not in self.valid_read_file_path:
            raise errors.MessageError('Server requests for loading invalid'
                    ' file: {}'.format(filename))

        with open(filename, "rb") as f:
            self._send_copy_data(f, self.buffer_size)
        self.connection.write(messages.EndOfBatchRequest())

    def _read_copy_data_response(self, is_stdin_copy=False):
        """Return True if the server wants us to load more data, false if we are done"""
        self._message = self.connection.read_expected_message(END_OF_BATCH_RESPONSES)
        # Check for rejections during this load
        while isinstance(self._message, messages.WriteFile):
            if self._message.filename == '':
                self._logger.info('COPY-LOCAL rejected row numbers: {}'.format(self._message.rejected_rows))
            elif self._message.filename in self.valid_write_file_path:
                self._message.write_to_disk(self.connection, self.buffer_size)
            else:
                raise errors.MessageError('Server requests for writing to'
                    ' invalid file path: {}'.format(self._message.filename))
            self._message = self.connection.read_expected_message(END_OF_BATCH_RESPONSES)

        # For STDIN copy, there will be no incoming message until we send
        # another EndOfBatchRequest or CopyDone
        if is_stdin_copy:
            self.connection.write(messages.CopyDone())  # End this copy
            self._message = self.connection.read_message()
            if isinstance(self._message, messages.ErrorResponse):
                raise errors.QueryError.from_error_response(self._message, self.operation)
            return False

        # For file copy, peek the next message
        self._message = self.connection.read_message()
        if isinstance(self._message, messages.LoadFile):
            # Indicate there are more local files to load
            return True
        elif isinstance(self._message, messages.ErrorResponse):
            raise errors.QueryError.from_error_response(self._message, self.operation)
        elif not isinstance(self._message, messages.CopyDoneResponse):
            raise errors.MessageError('Unexpected COPY FROM LOCAL state: {}'.format(
                                      type(self._message).__name__))
        return False

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
            self.description = self._message.get_description()
            self._deserializers = self._des.get_row_deserializers(self.description,
                                    {'unicode_error': self.unicode_error,
                                     'session_tz': self.connection.parameters.get('timezone', 'unknown'),
                                     'complex_types_enabled': self.connection.complex_types_enabled,})

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
                                             parameter_values, parameter_type_oids,
                                             self.connection.options['binary_transfer']))
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

