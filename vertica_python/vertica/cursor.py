

import re
import logging

from collections import OrderedDict
from builtins import str

import vertica_python.errors as errors

import vertica_python.vertica.messages as messages
from vertica_python.vertica.column import Column

logger = logging.getLogger('vertica')

class Cursor(object):
    def __init__(self, connection, cursor_type=None, unicode_error='strict'):
        self.connection = connection
        self.cursor_type = cursor_type
        self.unicode_error = unicode_error
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

    def __exit__(self, type_, value, traceback):
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
                    if isinstance(param, str):
                        v = adapt(param.encode('utf8')).getquoted()
                    else:
                        v = adapt(param).getquoted()

                    # Using a regex with word boundary to correctly handle params with similar names
                    # such as :s and :start
                    match_str = u':%s\\b' % str(key)
                    operation = re.sub(match_str, v.decode('utf-8'), operation, flags=re.UNICODE)
            elif isinstance(parameters, tuple):
                tlist = []
                for p in parameters:
                    if isinstance(p, str):
                        tlist.append(adapt(p.encode('utf8')).getquoted())
                    else:
                        tlist.append(adapt(p).getquoted())
                operation = operation % tuple(tlist)
            else:
                raise errors.Error("Argument 'parameters' must be dict or tuple")

        self.rowcount = -1

        self.connection.write(messages.Query(operation))

        # read messages until we hit an Error, DataRow or ReadyForQuery
        while True:
            message = self.connection.read_message()
            # save the message because there's no way to undo the read
            self._message = message
            if isinstance(message, messages.ErrorResponse):
                raise errors.QueryError.from_error_response(message, operation)
            elif isinstance(message, messages.RowDescription):
                self.description = list(map(lambda fd: Column(fd, self.unicode_error), message.fields))
            elif isinstance(message, messages.DataRow):
                break
            elif isinstance(message, messages.ReadyForQuery):
                break
            else:
                self.connection.process_message(message)

    def fetchone(self):
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
            return None
        elif isinstance(self._message, messages.CommandComplete):
            # there might be another set, read next message to find out
            self._message = self.connection.read_message()
            if isinstance(self._message, messages.RowDescription):
                # next row will be either a DataRow or CommandComplete
                self._message = self.connection.read_message()
                return True
            elif isinstance(self._message, messages.ReadyForQuery):
                return None
            else:
                raise errors.Error('Unexpected nextset() state after CommandComplete: ' + str(self._message))
        elif isinstance(self._message, messages.ReadyForQuery):
            # no more sets left to be read
            return None
        else:
            raise errors.Error('Unexpected nextset() state: ' + str(self._message))


    def setinputsizes(self):
        pass

    def setoutputsize(self, size, column=None):
        pass

    #
    # Non dbApi methods
    #
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

    def flush_to_command_complete(self):
        # if the last message isnt empty or CommandComplete, read messages until it is
        if(self._message is None
           or isinstance(self._message, messages.ReadyForQuery)
           or isinstance(self._message, messages.CommandComplete)):
            return

        while True:
            message = self.connection.read_message()
            if isinstance(message, messages.CommandComplete):
                self._message = message
                break


    # example:
    #
    # with open("/tmp/file.csv", "rb") as fs:
    #   cursor.copy("COPY table(field1,field2) FROM STDIN DELIMITER ',' ENCLOSED BY '\"'", fs, buffer_size=65536)
    #

    def copy(self, sql, data, **kwargs):

        if self.closed():
            raise errors.Error('Cursor is closed')

        self.flush_to_query_ready()

        self.connection.write(messages.Query(sql))

        while True:
            message = self.connection.read_message()

            if isinstance(message, messages.ErrorResponse):
                raise errors.QueryError.from_error_response(message, sql)

            self.connection.process_message(message=message)
            if isinstance(message, messages.ReadyForQuery):
                break
            elif isinstance(message, messages.CopyInResponse):

                #write stuff
                if not hasattr(data, "read"):
                    self.connection.write(messages.CopyData(data))
                else:
                    # treat data as stream
                    self.connection.write(messages.CopyStream(data, **kwargs))

                self.connection.write(messages.CopyDone())

        if self.error is not None:
            raise self.error

    #
    # Internal
    #

    def closed(self):
        return self._closed or self.connection.closed()

    def row_formatter(self, row_data):
        if self.cursor_type is None:
            return self.format_row_as_array(row_data)
        elif self.cursor_type in (list, 'list'):
            return self.format_row_as_array(row_data)
        elif self.cursor_type in (dict, 'dict'):
            return self.format_row_as_dict(row_data)
        else:
            raise Exception('Unrecognized cursor_type: %r' % self.cursor_type)

    def format_row_as_dict(self, row_data):
        return OrderedDict(
            (self.description[idx].name, self.description[idx].convert(value))
            for idx, value in enumerate(row_data.values)
        )

    def format_row_as_array(self, row_data):
        return [self.description[idx].convert(value)
                for idx, value in enumerate(row_data.values)]
