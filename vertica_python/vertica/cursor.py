from __future__ import print_function, division, absolute_import

import logging
import re

try:
    from collections import OrderedDict  # python 2.7+ / 3
except ImportError:
    from ordereddict import OrderedDict  # python 2.6

from io import IOBase

import six
# noinspection PyUnresolvedReferences,PyCompatibility
from builtins import str
from six import binary_type, text_type, string_types, BytesIO, StringIO

try:
    from psycopg2.extensions import QuotedString
except ImportError:
    class QuotedString(object):
        def __init__(self, s):
            raise ImportError("couldn't import psycopg2.extensions.QuotedString")

from .. import errors
from ..compat import as_text
from ..vertica import messages
from ..vertica.column import Column

logger = logging.getLogger('vertica')

UTF_8 = 'utf-8'

if six.PY2:
    # noinspection PyUnresolvedReferences
    file_type = (IOBase, file)
elif six.PY3:
    file_type = (IOBase,)


class Cursor(object):
    # NOTE: this is used in executemany and is here for pandas compatibility
    _insert_statement = re.compile(
        u"INSERT\\s+INTO"
        "\\s+((?P<schema>{id})\\.)?(?P<table>{id})"
        "\\s*\\(\\s*(?P<variables>({id}(\\s*,\\s*{id})*)?\\s*)\\)"
        "\\s+VALUES\\s*\\(\\s*(?P<values>.*)\\)".format(id=u"[a-zA-Z_][\\w\\d\\$_]*"), re.U | re.I)

    def __init__(self, connection, cursor_type=None, unicode_error=None):
        self.connection = connection
        self.cursor_type = cursor_type
        self.unicode_error = unicode_error if unicode_error is not None else 'strict'
        self._closed = False
        self._message = None
        self.operation = None

        self.error = None

        #
        # dbapi properties
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
        self._closed = True

    def cancel(self):
        if self.closed():
            raise errors.Error('Cursor is closed')

        self.connection.close()

    def execute(self, operation, parameters=None):
        self.operation = as_text(operation)

        if self.closed():
            raise errors.Error('Cursor is closed')

        self.flush_to_query_ready()

        if parameters:
            # TODO: quote = True for backward compatibility. see if should be False.
            operation = self.format_operation_with_parameters(operation, parameters, quote=True)

        self.rowcount = -1

        self.connection.write(messages.Query(operation))

        # read messages until we hit an Error, DataRow or ReadyForQuery
        self._message = self.connection.read_message()
        while True:
            if isinstance(self._message, messages.ErrorResponse):
                raise errors.QueryError.from_error_response(self._message, operation)
            elif isinstance(self._message, messages.RowDescription):
                self.description = [Column(fd, self.unicode_error) for fd in self._message.fields]
            elif isinstance(self._message, messages.DataRow):
                break
            elif isinstance(self._message, messages.ReadyForQuery):
                break
            elif isinstance(self._message, messages.CommandComplete):
                break
            else:
                self.connection.process_message(self._message)

            self._message = self.connection.read_message()

    def executemany(self, operation, seq_of_parameters):
        operation = as_text(operation)

        if not isinstance(seq_of_parameters, (list, tuple)):
            raise TypeError("seq_of_parameters should be list/tuple")

        m = self._insert_statement.match(operation)
        if m:
            schema = as_text(m.group('schema'))
            table = as_text(m.group('table'))
            variables = as_text(m.group('variables'))
            values = as_text(m.group('values'))
            if schema is not None:
                table = "%s.%s" % (schema, table)

            variables = ",".join([variable.strip() for variable in variables.split(",")])

            values = ",".join([value.strip() for value in values.split(",")])
            seq_of_values = [self.format_operation_with_parameters(values, parameters)
                             for parameters in seq_of_parameters]
            data = "\n".join(seq_of_values)

            copy_statement = (
                "COPY {table} ({variables}) FROM STDIN DELIMITER ',' ENCLOSED BY '\"' "
                "ENFORCELENGTH ABORT ON ERROR").format(table=table, variables=variables)

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
            elif isinstance(self._message, messages.ReadyForQuery):
                return None
            elif isinstance(self._message, messages.CommandComplete):
                return None
            else:
                self.connection.process_message(self._message)

            self._message = self.connection.read_message()

    def iterate(self):
        row = self.fetchone()
        while row:
            yield row
            row = self.fetchone()

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
        # skip any data for this set if exists
        self.flush_to_command_complete()

        if self._message is None:
            return False
        elif isinstance(self._message, messages.CommandComplete):
            # there might be another set, read next message to find out
            self._message = self.connection.read_message()
            if isinstance(self._message, messages.RowDescription):
                # next row will be either a DataRow or CommandComplete
                self._message = self.connection.read_message()
                return True
            elif isinstance(self._message, messages.ReadyForQuery):
                return False
            elif isinstance(self._message, messages.ErrorResponse):
                raise errors.QueryError.from_error_response(self._message, self.operation)
            else:
                raise errors.Error(
                    'Unexpected nextset() state after CommandComplete: {0}'.format(self._message))
        elif isinstance(self._message, messages.ReadyForQuery):
            # no more sets left to be read
            return False
        else:
            raise errors.Error('Unexpected nextset() state: {0}'.format(self._message))

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    #############################################
    # non-dbapi methods
    #############################################
    def flush_to_query_ready(self):
        # if the last message isn't empty or ReadyForQuery, read all remaining messages
        if self._message is None \
                or isinstance(self._message, messages.ReadyForQuery):
            return

        while True:
            message = self.connection.read_message()
            if isinstance(message, messages.ReadyForQuery):
                self.connection.transaction_status = message.transaction_status
                self._message = message
                break

    def flush_to_command_complete(self):
        # if the last message isn't empty or CommandComplete, read messages until it is
        if self._message is None or isinstance(self._message, (messages.ReadyForQuery,
                                                               messages.CommandComplete)):
            return

        while True:
            message = self.connection.read_message()
            if isinstance(message, messages.CommandComplete):
                self._message = message
                break

    def copy(self, sql, data, **kwargs):
        """
        
        EXAMPLE:
        >> with open("/tmp/file.csv", "rb") as fs:
        >>     cursor.copy("COPY table(field1,field2) FROM STDIN DELIMITER ',' ENCLOSED BY '\"'",
        >>                 fs, buffer_size=65536)

        """
        sql = as_text(sql)

        if self.closed():
            raise errors.Error('Cursor is closed')

        self.flush_to_query_ready()

        if isinstance(data, binary_type):
            stream = BytesIO(data)
        elif isinstance(data, text_type):
            stream = StringIO(data)
        elif isinstance(data, file_type):
            stream = data
        else:
            raise TypeError("Not valid type of data {0}".format(type(data)))

        self.connection.write(messages.Query(sql))

        while True:
            message = self.connection.read_message()

            if isinstance(message, messages.ErrorResponse):
                raise errors.QueryError.from_error_response(message, sql)

            self.connection.process_message(message=message)

            if isinstance(message, messages.ReadyForQuery):
                break
            elif isinstance(message, messages.CopyInResponse):
                self.connection.write(messages.CopyStream(stream, **kwargs))
                self.connection.write(messages.CopyDone())

        if self.error is not None:
            raise self.error

    def closed(self):
        return self._closed or self.connection.closed()

    #############################################
    # internal
    #############################################
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
    def format_operation_with_parameters(self, operation, parameters, quote=True):
        operation = as_text(operation)

        if isinstance(parameters, dict):
            for key, param in six.iteritems(parameters):
                if not isinstance(key, string_types):
                    key = str(key)
                key = as_text(key)

                if isinstance(param, string_types):
                    param = as_text(param)
                    if quote:
                        param = self.format_quote(param)
                elif param is None:
                    param = 'NULL'
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
                if isinstance(param, string_types):
                    param = as_text(param)
                    if quote:
                        param = self.format_quote(param)
                elif param is None:
                    param = 'NULL'
                else:
                    param = str(param)
                value = as_text(param)

                tlist.append(value)

            operation = operation % tuple(tlist)
        else:
            raise errors.Error("Argument 'parameters' must be dict or tuple")

        return operation

    def format_quote(self, param):
        # TODO Make sure adapt() behaves properly
        return QuotedString(param.encode(UTF_8, self.unicode_error)).getquoted()
