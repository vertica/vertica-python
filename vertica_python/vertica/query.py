from __future__ import absolute_import

import vertica_python.vertica.messages as messages

from vertica_python.vertica.result import Result
from vertica_python.vertica.error import QueryError, EmptyQueryError

class Query(object):

    def __init__(self, connection, sql, options={}):
        self.connection = connection
        self.sql = sql

        if options.get('row_style') is not None:
            self.row_style = options.get('row_style')
        elif self.connection.row_style is not None:
            self.row_style = self.connection.row_style
        else:
            self.row_style = 'hash'

        self.row_handler = options.get('row_handler')
        self.copy_handler = options.get('copy_handler')

        self.error = None
        self.result = Result(self.row_style)

    def run(self):
        self.connection.write(messages.Query(self.sql))

        while True:
            message = self.connection.read_message()
            self._process_message(message=message)
            if isinstance(message, messages.ReadyForQuery):
                break
        if self.error is not None:
            raise self.error

        return self.result

    def write(self, data):
        self.connection.write(messages.CopyData(data))
        return self

    def __lshift__(self, data):
        self.write(data)

    def __str__(self):
        return self.sql

    def _process_message(self, message):
        if isinstance(message, messages.ErrorResponse):
            self.error = QueryError.from_error_response(message, self.sql)
        elif isinstance(message, messages.EmptyQueryResponse):
            self.error = EmptyQueryError("A SQL string was expected, but the given string was blank or only contained SQL comments.")
        elif isinstance(message, messages.CopyInResponse):
            self._handle_copy_from_stdin()
        elif isinstance(message, messages.RowDescription):
            self.result.descriptions(message)
        elif isinstance(message, messages.DataRow):
            self._handle_datarow(message)
        elif isinstance(message, messages.CommandComplete):
            self.result.tag = message.tag
        else:
            self.connection.process_message(message)

    def _handle_copy_from_stdin(self):
        if self.copy_handler is None:
            self.connection.write(messages.CopyFail("No copy handler provided"))

        try:
            if self.copy_handler(self) == 'rollback':
                self.connection.write(messages.CopyFail("rollback"))
            else:
                self.connection.write(messages.CopyDone())
        except Exception, e:
            self.connection.write(messages.CopyFail(e))
            raise

    def _handle_datarow(self, datarow_message):
        record = self.result.format_row(datarow_message)
        if self._buffer_rows() is True:
            self.result.add_row(record)
        if self.row_handler is not None:
            self.row_handler(record)

    def _buffer_rows(self):
        return self.row_handler is None and self.copy_handler is None
