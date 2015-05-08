from __future__ import absolute_import

import re
import logging

import vertica_python.errors as errors

import vertica_python.vertica.messages as messages
from vertica_python.vertica.column import Column

logger = logging.getLogger('vertica')

class Cursor(object):
    def __init__(self, connection, cursor_type=None):
        self.connection = connection
        self.cursor_type = cursor_type
        self._closed = False
        self._message = None

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

    def callproc(self, procname, parameters=None):
        raise errors.NotSupportedError('Cursor.callproc() is not implemented')

    def close(self):
        self._closed = True

    def execute(self, operation, parameters=None):
        if self.closed():
            raise errors.Error('Cursor is closed')

        self.flush_to_query_ready()

        if parameters:
            # # optional requirement
            from psycopg2.extensions import adapt

            if isinstance(parameters, dict):
                for key in parameters:
                    param = parameters[key]
                    # Make sure adapt() behaves properly
                    if isinstance(param, unicode):
                        v = adapt(param.encode('utf8')).getquoted()
                    else:
                        v = adapt(param).getquoted()

                    # Using a regex with word boundary to correctly handle params with similar names
                    # such as :s and :start
                    match_str = u':%s\\b' % unicode(key)
                    operation = re.sub(match_str, v.decode('utf-8'), operation, re.UNICODE)
            elif isinstance(parameters, tuple):
                tlist = []
                for p in parameters:
                    if isinstance(p, unicode):
                        tlist.append(adapt(p.encode('utf8')).getquoted())
                    else:
                        tlist.append(adapt(p).getquoted())
                operation = operation % tuple(tlist)
            else:
                raise errors.Error("Argument 'parameters' must be dict or tuple")

        self.rowcount = 0

        self.connection.write(messages.Query(operation))

        # read messages until we hit an Error, DataRow or ReadyForQuery
        while True:
            message = self.connection.read_message()
            # save the message because there's no way to undo the read
            self._message = message
            if isinstance(message, messages.ErrorResponse):
                raise errors.QueryError.from_error_response(message, operation)
            elif isinstance(message, messages.RowDescription):
                self.description = map(lambda fd: Column(fd), message.fields)
            elif (isinstance(message, messages.DataRow)
                   or isinstance(message, messages.ReadyForQuery)):
                break
            else:
                self.connection.process_message(message)

    def fetchone(self):
        if isinstance(self._message, messages.DataRow):
            self.rowcount += 1
            row = self.row_formatter(self._message)
            # fetch next message
            self._message = self.connection.read_message()
            return row
        else:
            self.connection.process_message(self._message)

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

    def setinputsizes(self):
        pass

    def setoutputsize(self, size, column=None):
        pass

    #
    # Non dbApi methods
    #
    # todo: input stream
    def flush_to_query_ready(self):
        # if the last message isnt empty or ReadyForQuery, read all remaining messages
        if(self._message is None
           or isinstance(self._message, messages.ReadyForQuery)):
            return

        while True:
            message = self.connection.read_message()
            if isinstance(message, messages.ReadyForQuery):
                self.connection.transaction_status = message.transaction_status
                self._message = message
                break

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
                raise errors.QueryError.from_error_response(message, sql)
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

    def row_formatter(self, row_data):
        if not self.cursor_type:
            return self.format_row_as_array(row_data)
        elif self.cursor_type == 'list':
            return self.format_row_as_array(row_data)
        elif self.cursor_type == 'dict':
            return self.format_row_as_dict(row_data)
            # throw some error

    def format_row_as_dict(self, row_data):
        return dict(
            (self.description[idx].name, self.description[idx].convert(value))
            for idx, value in enumerate(row_data.values)
        )

    def format_row_as_array(self, row_data):
        return [self.description[idx].convert(value)
                for idx, value in enumerate(row_data.values)]
