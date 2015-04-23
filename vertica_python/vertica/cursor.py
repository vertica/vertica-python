from __future__ import absolute_import

import vertica_python.errors as errors

import vertica_python.vertica.messages as messages
from vertica_python.vertica.column import Column


class Cursor(object):
    def __init__(self, connection, cursor_type=None):
        self.connection = connection
        self.cursor_type = cursor_type
        self._closed = False

        self.last_execution = None
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
        if self.last_execution:
            # ToDo: can just empty the message buffer in connection if easy to do
            self.connection.reset_connection()
        self.last_execution = operation
        self.connection.write(messages.Query(operation))

        # Fetch the schema
        message = self.connection.read_message()
        if isinstance(message, messages.ErrorResponse):
            raise errors.QueryError.from_error_response(message, self.last_execution)
        elif not isinstance(message, messages.RowDescription):
            print("Message type: {0}".format(type(message)))
            print(message)
            raise errors.QueryError("Unexpected message type", self.last_execution)
        self.description = map(lambda fd: Column(fd), message.fields)

    def iterate(self):
        while True:
            message = self.connection.read_message()
            if isinstance(message, messages.DataRow):
                self.rowcount += 1
                yield self.row_formatter(message)
            else:
                # REVIEW: any other case should we check?
                break

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
            if isinstance(message, messages.ErrorResponse):
                raise errors.QueryError.from_error_response(message, self.last_execution)
            elif isinstance(message, messages.ReadyForQuery):
                break
            elif isinstance(message, messages.CopyInResponse):
                # write stuff
                for line in datagen:
                    self.connection.write(messages.CopyData(line))
                self.connection.write(messages.CopyDone())

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
        message = self.connection.read_message()
        if isinstance(message, messages.DataRow):
            self.rowcount += 1
            return self.row_formatter(message)

        return None

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