# Copyright (c) 2018-2024 Open Text.
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


from __future__ import print_function, division, absolute_import, annotations

import datetime
import glob
import inspect
import os
import re
import sys
import traceback
import warnings
from decimal import Decimal
from io import IOBase, BytesIO, StringIO
from math import isnan
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
    from typing import IO, Any, AnyStr, Callable, Dict, Generator, List, NoReturn, Optional, Sequence, Tuple, Type, TypeVar, Union
    from typing_extensions import Self
    from .connection import Connection
    from logging import Logger
    T = TypeVar('T')

from .. import errors, os_utils
from ..compat import as_text
from ..vertica import messages
from ..vertica.column import Column
from ..vertica.deserializer import Deserializer
from ..vertica.messages.message import BackendMessage


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

    def __init__(self,
                 connection: Connection,
                 logger: Logger,
                 cursor_type: Union[None, str, Type[List[Any]], Type[Dict[Any, Any]]] = None,
                 unicode_error: Optional[str] = None) -> None:
        self.connection = connection
        self._logger = logger
        self.cursor_type = cursor_type
        self.unicode_error = unicode_error if unicode_error is not None else 'strict'
        self._closed = False
        self._message = None
        self.operation = None
        self.prepared_sql = None  # last statement been prepared
        self.prepared_name = "s0"
        self._sql_literal_adapters = {}
        self._disable_sqldata_converter = False
        self._sqldata_converters = {}
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
    def __enter__(self) -> Self:
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
    def callproc(self, procname, parameters=None) -> NoReturn:
        raise errors.NotSupportedError('Cursor.callproc() is not implemented')

    def close(self) -> None:
        """Close the cursor now."""
        self._logger.info('Close the cursor')
        if not self.closed() and self.prepared_sql:
            self._close_prepared_statement()
        self._closed = True

    @handle_ctrl_c
    def execute(self, operation: str,
                parameters: Optional[Union[List[Any], Tuple[Any], Dict[str, Any]]] = None,
                use_prepared_statements: Optional[bool] = None,
                copy_stdin: Optional[Union[IO[AnyStr], List[IO[AnyStr]]]] = None,
                buffer_size: int = DEFAULT_BUFFER_SIZE) -> Self:
        """Execute a query or command to the database."""
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
            #################################################################
            # Execute the SQL as prepared statement (server-side bindings)
            #################################################################
            if parameters is not None:
                if not isinstance(parameters, (list, tuple)):
                    raise TypeError("Execute parameters should be a list/tuple")
                elif parameters and '?' not in operation:
                    raise ValueError(f'Invalid SQL: {operation}'
                        '\nHINT: When use_prepared_statements=True, variables in SQL should be specified with '
                        'question mark (?) placeholders. Positional format (%s) placeholders have to be used '
                        'with use_prepared_statements=False setting.')

            # If the SQL has not been prepared, prepare the SQL
            if operation != self.prepared_sql:
                self._prepare(operation)
                self.prepared_sql = operation  # the prepared statement is kept

            # Bind the parameters and execute
            self._execute_prepared_statement([parameters])
        else:
            #################################################################
            # Execute the SQL directly (client-side bindings)
            #################################################################
            if parameters:
                operation = self.format_operation_with_parameters(operation, parameters)
            self._execute_simple_query(operation)

        return self

    @handle_ctrl_c
    def executemany(self,
                    operation: str,
                    seq_of_parameters: Sequence[Union[List[Any], Tuple[Any], Dict[str, Any]]],
                    use_prepared_statements: Optional[bool] = None) -> None:
        """Execute the same command with a sequence of input data."""

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
            #################################################################
            # Execute the SQL as prepared statement (server-side bindings)
            #################################################################
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
            #################################################################
            # Rewrite the INSERT SQL into a COPY statement
            #################################################################
            m = self._insert_statement.match(operation)
            if m:
                target = as_text(m.group('target'))

                variables = as_text(m.group('variables'))
                variables = ",".join([variable.strip().strip('"') for variable in variables.split(",")])

                values = as_text(m.group('values'))
                values = "|".join([value.strip().strip('"') for value in values.split(",")])
                seq_of_values = [self.format_operation_with_parameters(values, parameters, is_copy_data=True)
                                 for parameters in seq_of_parameters]
                data = "\n".join(seq_of_values)

                copy_statement = (
                    u"COPY {0} ({1}) FROM STDIN "
                    u"ENCLOSED BY '''' "  # '/r' will have trouble if ENCLOSED BY is not set
                    u"ENFORCELENGTH ABORT ON ERROR{2}").format(target, variables,
                    " NO COMMIT" if not self.connection.autocommit else '')

                self.copy(copy_statement, data)
            else:
                raise NotImplementedError(
                    "executemany is implemented for simple INSERT statements only")

    def fetchone(self) -> Optional[Union[List[Any], OrderedDict[str, Any]]]:
        """Return the next record from the current statement result set."""
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
                self._deserializers = self.get_deserializers()
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

    def fetchmany(self, size: Optional[int] = None) -> List[Union[List[Any], OrderedDict[str, Any]]]:
        """Return the next `size` records from the current statement result set.
        `size` default to `cursor.arraysize` if not specified.
        """
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

    def fetchall(self) -> List[Union[List[Any], OrderedDict[str, Any]]]:
        """Return all the remaining records from the current statement result set."""
        return list(self.iterate())

    def nextset(self) -> bool:
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
                self._deserializers = self.get_deserializers()
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
            elif isinstance(self._message, messages.CopyInResponse):
                raise errors.MessageError(
                    'Unexpected nextset() state after END_OF_RESULT_RESPONSES: {self._message}\n'
                    'HINT: Do you pass multiple COPY statements into Cursor.copy()?')
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

    def setinputsizes(self, sizes) -> None:
        pass

    def setoutputsize(self, size, column=None) -> None:
        pass

    #############################################
    # non-dbapi methods
    #############################################
    def closed(self) -> bool:
        """Returns True if the cursor is closed."""
        return self._closed or self.connection.closed()

    def cancel(self) -> NoReturn:
        """Cancel is a session-level operation, cursor-level API does not make
        sense. Keep this API for backward compatibility.
        """
        raise errors.NotSupportedError(
            'Cursor.cancel() is deprecated. Call Connection.cancel() '
            'to cancel the current database operation.')

    def iterate(self) -> Generator[Union[List[Any], OrderedDict[str, Any]], None, None]:
        """Yield the next record from the current statement result set."""
        row = self.fetchone()
        while row:
            yield row
            row = self.fetchone()

    def copy(self, sql: str, data: Union[IO[AnyStr], bytes, str], **kwargs: Any) -> None:
        """
        Execute a "COPY FROM STDIN" SQL.

        EXAMPLE:
        ```
        >> with open("/tmp/file.csv", "rb") as fs:
        >>     cursor.copy("COPY table(field1,field2) FROM STDIN DELIMITER ',' ENCLOSED BY ''''",
        >>                 fs, buffer_size=65536)
        ```
        """
        sql = as_text(sql)
        self.operation = sql

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

        self._logger.info(u'Execute COPY statement: [{}]'.format(sql))
        # Execute a `COPY FROM STDIN` SQL statement
        self.connection.write(messages.Query(sql))

        self.buffer_size = kwargs.get('buffer_size', DEFAULT_BUFFER_SIZE)

        while True:
            message = self.connection.read_message()

            self._message = message
            if isinstance(message, messages.ErrorResponse):
                raise errors.QueryError.from_error_response(message, sql)
            elif isinstance(message, messages.ReadyForQuery):
                break
            elif isinstance(message, messages.CommandComplete):
                break
            elif isinstance(message, messages.CopyInResponse):
                try:
                    self._send_copy_data(stream, self.buffer_size)
                except Exception as e:
                    # COPY termination: report the cause of failure to the backend
                    self.connection.write(messages.CopyFail(str(e)))
                    self._logger.error(str(e))
                    self.flush_to_query_ready()
                    raise errors.DataError('Failed to send a COPY data stream: {}'.format(str(e)))

                # Successful termination for COPY
                self.connection.write(messages.CopyDone())
            elif isinstance(message, messages.RowDescription):
                raise errors.MessageError(f'Unexpected message: {message}\n'
                     f'HINT: Query for Cursor.copy() should be a `COPY FROM STDIN` SQL statement.'
                     ' `COPY FROM LOCAL` should be executed with Cursor.execute().\n'
                     f'SQL: {sql}')
            else:
                raise errors.MessageError(f'Unexpected message: {message}')

    def object_to_sql_literal(self, py_obj: Any) -> str:
        """Returns the SQL literal string converted from a Python object."""
        return self.object_to_string(py_obj, False)

    def register_sql_literal_adapter(self, obj_type: T, adapter_func: Callable[[T], str]) -> None:
        """Register a sql literal adapter, which adapt a Python type/class to SQL literals."""
        if not callable(adapter_func):
            raise TypeError("Cannot register this sql literal adapter. The adapter is not callable.")
        self._sql_literal_adapters[obj_type] = adapter_func

    @property
    def disable_sqldata_converter(self) -> bool:
        return self._disable_sqldata_converter

    @disable_sqldata_converter.setter
    def disable_sqldata_converter(self, value: bool) -> None:
        """By default, the client does data conversions for query results:
        reading a bytes sequence from server and creating a Python object out of it.
        If set to True, bypass conversions from SQL type raw data to the native Python object.
        """
        self._disable_sqldata_converter = bool(value)

    def register_sqldata_converter(self, oid: int, converter_func: Callable[[bytes, Dict[str, Any]], Any]) -> None:
        """Customize how SQL data values are converted to Python objects when query results are returned."""
        if not isinstance(oid, int):
            raise TypeError(f"sqldata converters should be registered on oid integer, got {oid} instead.")

        if not callable(converter_func):
            raise TypeError("Cannot register this sqldata converter. The converter is not callable.")

        # For an oid, transfer format (BINARY/TEXT) is fixed in a connection
        self._sqldata_converters[oid] = converter_func
        # For prepared statements, need to reset self._deserializers
        if self.description: self._deserializers = self.get_deserializers()

    def unregister_sqldata_converter(self, oid: int) -> None:
        """Cancel customized SQL data values converter and use the default converter."""
        if oid in self._sqldata_converters:
            del self._sqldata_converters[oid]
            # For prepared statements, need to reset self._deserializers
            if self.description: self._deserializers = self.get_deserializers()
        else:
            no_such_oid = f'Nothing was unregistered (oid={oid})'
            warnings.warn(no_such_oid)


    #############################################
    # internal
    #############################################
    def get_deserializers(self):
        return self._des.get_row_deserializers(
                  self.description, self._sqldata_converters,
                  {'unicode_error': self.unicode_error,
                   'session_tz': self.connection.parameters.get('timezone', 'unknown'),
                   'complex_types_enabled': self.connection.complex_types_enabled,}
               )

    def flush_to_query_ready(self) -> None:
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

    def flush_to_end_of_result(self) -> None:
        # if the last message isn't empty or END_OF_RESULT_RESPONSES,
        # read messages until it is
        if (self._message is None or
            isinstance(self._message, messages.ReadyForQuery) or
            isinstance(self._message, END_OF_RESULT_RESPONSES)):
            return

        while True:
            message = self.connection.read_message()
            if (isinstance(message, messages.ReadyForQuery) or
                isinstance(message, END_OF_RESULT_RESPONSES)):
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

    def format_row_as_dict(self, row_data) -> OrderedDict[str, Any]:
        if self._disable_sqldata_converter:
            return OrderedDict((descr.name, value)
                    for descr, value in zip(self.description, row_data.values))
        return OrderedDict(
            (descr.name, convert(value))
            for descr, convert, value in zip(self.description, self._deserializers, row_data.values)
        )

    def format_row_as_array(self, row_data) -> List[Any]:
        if self._disable_sqldata_converter:
            return row_data.values
        return [convert(value)
                for convert, value in zip(self._deserializers, row_data.values)]

    def object_to_string(self, py_obj: Any, is_copy_data: bool, is_collection: bool = False) -> str:
        """Return the SQL representation of the object as a string"""
        if type(py_obj) in self._sql_literal_adapters and not is_copy_data:
            adapter = self._sql_literal_adapters[type(py_obj)]
            result = adapter(py_obj)
            if not isinstance(result, (str, bytes)):
                raise TypeError("Unexpected return type of {} adapter: {}, expected a string type."
                    .format(type(py_obj), type(result)))
            return as_text(result)

        if isinstance(py_obj, type(None)):
            return '' if is_copy_data and not is_collection else 'NULL'
        elif isinstance(py_obj, bool):
            return str(py_obj)
        elif isinstance(py_obj, (str, bytes)):
            return self.format_quote(as_text(py_obj), is_copy_data, is_collection)
        elif isinstance(py_obj, (int, Decimal)):
            return str(py_obj)
        elif isinstance(py_obj, float):
            if not is_copy_data and py_obj in (float('Inf'), float('-Inf')) or isnan(py_obj):
                return f"'{str(py_obj)}'::FLOAT"
            return str(py_obj)
        elif isinstance(py_obj, tuple):  # tuple and namedtuple
            elements = [None] * len(py_obj)
            for i in range(len(py_obj)):
                elements[i] = self.object_to_string(py_obj[i], is_copy_data)
            return "(" + ",".join(elements) + ")"
        elif isinstance(py_obj, list):
            elements = [None] * len(py_obj)
            if is_copy_data:
                for i in range(len(py_obj)):
                    elements[i] = self.object_to_string(py_obj[i], True, True)
                return f'[{",".join(elements)}]'
            else:
                for i in range(len(py_obj)):
                    elements[i] = self.object_to_string(py_obj[i], False)
                # Use the ARRAY keyword to construct an array value
                return f'ARRAY[{",".join(elements)}]'
        elif isinstance(py_obj, set):
            elements = [None] * len(py_obj)
            i = 0
            if is_copy_data:
                for o in py_obj:
                    elements[i] = self.object_to_string(o, True, True)
                    i += 1
                return f'[{",".join(elements)}]'
            else:
                for o in py_obj:
                    elements[i] = self.object_to_string(o, False)
                    i += 1
                # Use the SET keyword to construct a set value
                return f'SET[{",".join(elements)}]'
        elif isinstance(py_obj, dict) and not is_copy_data:
            elements = [None] * len(py_obj)
            i = 0
            for k, v in py_obj.items():
                elements[i] = self.object_to_string(v, False) + f' AS "{k}"'
                i += 1
            # Use the ROW keyword to construct a row value
            return f'ROW({",".join(elements)})'
        elif isinstance(py_obj, (datetime.datetime, datetime.date, datetime.time, UUID)):
            return self.format_quote(as_text(str(py_obj)), is_copy_data, is_collection)
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
    def format_operation_with_parameters(self, operation: str, parameters: Union[List[Any], Tuple[Any], Dict[str, Any]], is_copy_data: bool = False) -> str:
        if isinstance(parameters, dict):
            if parameters and ':' not in operation and os.environ.get('VERTICA_PYTHON_IGNORE_NAMED_PARAMETER_CHECK') != '1':
                raise ValueError(f'Invalid SQL: {operation}'
                    "\nHINT: When argument 'parameters' is a dict, variables in SQL should be specified with named (:name) placeholders."
                    " If you use a dict to represent the value of a ROW type column, enclose the dict with brackets('[]') to construct a list.")
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
            if parameters and '%s' not in operation:
                raise ValueError(f'Invalid SQL: {operation}'
                    "\nHINT: When argument 'parameters' is a tuple/list, "
                    'variables in SQL should be specified with positional format (%s) placeholders. '
                    'Question mark (?) placeholders have to be used with use_prepared_statements=True setting.')
            tlist = []
            for param in parameters:
                value = self.object_to_string(param, is_copy_data)
                tlist.append(value)
            operation = operation % tuple(tlist)
        else:
            raise TypeError("Argument 'parameters' must be dict or tuple/list")

        return operation

    def format_quote(self, param: str, is_copy_data: bool, is_collection: bool) -> str:
        if is_collection: # COPY COLLECTIONENCLOSE
            s = list(param)
            for i, c in enumerate(param):
                if c in '\\\n\"':
                    s[i] = "\\" + c
            return u'"{0}"'.format(u"".join(s))
        elif is_copy_data: # COPY ENCLOSED BY
            s = list(param)
            for i, c in enumerate(param):
                if c in '\\|\n\'':
                    s[i] = "\\" + c
            return u"'{0}'".format(u"".join(s))
        else:
            return u"'{0}'".format(param.replace(u"'", u"''"))

    def _execute_simple_query(self, query: str) -> None:
        """
        Send the query to the server using the simple query protocol.
        """
        self._logger.info(u'Execute simple query: [{}]'.format(query))

        # All of the statements in the query are sent here in a single message
        self.connection.write(messages.Query(query))

        # The first response could be a number of things:
        #   ErrorResponse: Something went wrong on the server.
        #   EmptyQueryResponse: The query being executed is empty. (e.g. the string "--comment")
        #   RowDescription: This is the "normal" case when executing a query.
        #                   It marks the start of the results.
        #   CommandComplete: This occurs when executing DDL/transactions.
        self._message = self.connection.read_message()
        if isinstance(self._message, messages.ErrorResponse):
            raise errors.QueryError.from_error_response(self._message, query)
        elif isinstance(self._message, messages.RowDescription):
            self.description = self._message.get_description()
            self._deserializers = self.get_deserializers()
            self._message = self.connection.read_message()
            if isinstance(self._message, messages.ErrorResponse):
                raise errors.QueryError.from_error_response(self._message, query)
            elif isinstance(self._message, messages.VerifyFiles):
                self._handle_copy_local_protocol()

    def _handle_copy_local_protocol(self) -> None:
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

            self.connection.write(messages.VerifiedFiles(self.valid_read_file_path,
                                  self.connection.parameters.get('protocol_version', 0)))
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

    def _send_copy_data(self, stream, buffer_size) -> None:
        # Send zero or more CopyData messages, forming a stream of input data
        while True:
            chunk = stream.read(buffer_size)
            if not chunk:
                break
            self.connection.write(messages.CopyData(chunk, self.unicode_error))

    def _send_copy_file_data(self) -> None:
        filename = self._message.filename
        self._logger.info('Sending {} data to server'.format(filename))

        if filename not in self.valid_read_file_path:
            raise errors.MessageError('Server requests for loading invalid'
                    ' file: {}'.format(filename))

        with open(filename, "rb") as f:
            self._send_copy_data(f, self.buffer_size)
        self.connection.write(messages.EndOfBatchRequest())

    def _read_copy_data_response(self, is_stdin_copy: bool = False) -> bool:
        """Returns True if the server wants us to load more data, False if we are done."""
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

    def _error_handler(self, msg: BackendMessage) -> NoReturn:
        self.connection.write(messages.Sync())
        raise errors.QueryError.from_error_response(msg, self.operation)

    def _prepare(self, query: str) -> None:
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
            self._deserializers = self.get_deserializers()

        # Read expected message: CommandDescription
        self._message = self.connection.read_expected_message(messages.CommandDescription, self._error_handler)
        if len(self._message.command_tag) == 0:
            msg = 'The statement being prepared is empty'
            self._logger.error(msg)
            self.connection.write(messages.Sync())
            raise errors.EmptyQueryError(msg)

        self._logger.info('Finish preparing the statement')

    def _execute_prepared_statement(self, list_of_parameter_values: Sequence[Any]) -> None:
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

    def _close_prepared_statement(self) -> None:
        """
        Close the prepared statement on the server.
        """
        self.prepared_sql = None
        self.flush_to_query_ready()
        self.connection.write(messages.Close('prepared_statement', self.prepared_name))
        self.connection.write(messages.Flush())
        self._message = self.connection.read_expected_message(messages.CloseComplete)
        self.connection.write(messages.Sync())

