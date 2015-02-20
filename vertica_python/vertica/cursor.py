from __future__ import absolute_import

import collections

import vertica_python.errors as errors

import vertica_python.vertica.messages as messages
from vertica_python.vertica.column import Column


class Cursor(object):

    def __init__(self, connection, cursor_type=None, row_handler=None):
        self.connection = connection
        self.cursor_type = cursor_type
        self.row_handler = row_handler
        self._closed = False

        self.last_execution = None
        self.buffered_rows = collections.deque()
        self.error = None

        #
        # dbApi properties
        #
        self.description = None
        self.rowcount = -1
        self.arraysize = 1

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    #
    # dbApi methods
    #

    def callproc(procname, parameters=None):
        raise errors.NotSupportedError('Cursor.callproc() is not implemented')

    def close(self):
        self._closed = True

    def execute(self, operation, parameters=None):

        if self.closed():
            raise errors.Error('Cursor is closed')

        if parameters:
            # optional requirement
            from psycopg2.extensions import adapt

            if isinstance(parameters, dict):
                for key in parameters:
                    v = adapt(parameters[key]).getquoted()
                    operation = operation.replace(':' + key, v)
            elif isinstance(parameters, tuple):
                operation = operation % tuple(adapt(p).getquoted() for p in parameters)
            else:
                raise errors.Error("Argument 'parameters' must be dict or tuple")

        self.rowcount = 0
        self.buffered_rows = collections.deque()
        self.last_execution = operation
        self.connection.write(messages.Query(operation))

        self.fetch_rows()

        if self.error is not None:
            raise self.error

    def executemany(self, operation, seq_of_parameters):
        raise errors.NotSupportedError('Cursor.executemany() is not implemented')

    def fetchone(self):
        return self.get_one_row()

    def fetchmany(self, size=None):
        if not size:
            size = self.arraysize
        results = []
        while True:
            row = self.get_one_row()
            if not row:
                break
            results.append(row)
            if len(results) >= size:
                break
        return results

    def fetchall(self):
        results = []
        while True:
            row = self.get_one_row()
            if not row:
                break
            results.append(row)
        return results

    def nextset(self):
        raise errors.NotSupportedError('Cursor.nextset() is not implemented')

    def setinputsizes(self):
        pass

    def setoutputsize(self, size, column=None):
        pass

    #
    # Non dbApi methods
    #
    # todo: input stream
    def copy(self, sql, data):
        # Legacy support
        self.copy_string(sql, data)

    def _copy_internal(self, sql, datagen):
        if self.closed():
            raise errors.Error('Cursor is closed')

        self.connection.write(messages.Query(sql))

        while True:
            message = self.connection.read_message()
            self._process_message(message=message)
            if isinstance(message, messages.ReadyForQuery):
                break
            elif isinstance(message, messages.CopyInResponse):
                # write stuff
                for line in datagen:
                    self.connection.write(messages.CopyData(line))
                self.connection.write(messages.CopyDone())

        if self.error is not None:
            raise self.error

    def copy_string(self, sql, data):
        self._copy_internal(sql, [data])


    def copy_file(self, sql, data, decoder=None):
        if decoder is None:
            self._copy_internal(sql, data)
        else:
            self._copy_internal(sql, (line.decode(decoder) for line in data))


    #
    # Internal
    #

    def closed(self):
        return self._closed or self.connection.closed()

    def get_one_row(self):
        if len(self.buffered_rows) >= 1:
            return self.buffered_rows.popleft()

        return None

    def fetch_rows(self):
        while True:
            message = self.connection.read_message()
            self._process_message(message=message)
            if isinstance(message, messages.ReadyForQuery):
                break

    def _process_message(self, message):
        if isinstance(message, messages.ErrorResponse):
            # what am i doing with self.error?
            self.error = errors.QueryError.from_error_response(message, self.last_execution)
        elif isinstance(message, messages.EmptyQueryResponse):
            self.error = errors.EmptyQueryError("A SQL string was expected, but the given string was blank or only contained SQL comments.")
        elif isinstance(message, messages.CopyInResponse):
            pass
        elif isinstance(message, messages.RowDescription):
            self.set_description(message)
        elif isinstance(message, messages.DataRow):
            self._handle_datarow(message)
        elif isinstance(message, messages.CommandComplete):
            pass
#            self.result.tag = message.tag
        else:
            self.connection.process_message(message)
        return None

    # sets column meta data
    def set_description(self, message):
        self.description = map(lambda fd: Column(fd), message.fields)

    def _handle_datarow(self, datarow_message):
        row = self.row_formatter(datarow_message)
        if self.row_handler:
            self.row_handler(row)
        else:
            self.buffered_rows.append(row)
            self.rowcount += 1

    def row_formatter(self, row_data):
        if not self.cursor_type:
            return self.format_row_as_array(row_data)
        elif self.cursor_type == 'list':
            return self.format_row_as_array(row_data)
        elif self.cursor_type == 'dict':
            return self.format_row_as_dict(row_data)
        # throw some error

    def format_row_as_dict(self, row_data):
        return {
            self.description[idx].name: self.description[idx].convert(value)
            for idx, value in enumerate(row_data.values)
        }

    def format_row_as_array(self, row_data):
        return [self.description[idx].convert(value)
                for idx, value in enumerate(row_data.values)]
