from __future__ import absolute_import

import os
import select
import socket
import ssl

from struct import unpack

import vertica_python.vertica.messages as messages

from vertica_python.vertica.messages.message import BackendMessage
from vertica_python.vertica.query import Query
from vertica_python.vertica.error import ConnectionError, InterruptImpossible, SSLNotSupported
from vertica_python.vertica.error import MessageError, SynchronizeError, TimedOutError

class Connection(object):

    @classmethod
    def cancel(cls, existing_conn):
        existing_conn.cancel_conn()

    def __init__(self, options={}):
        self.reset_values()

        self.options = {}

        for key, value in options.iteritems():
            if value is not None:
                self.options[key] = value

        self.options['port'] = 5433 if self.options.get('port') is None else self.options['port']
        self.options['read_timeout'] = 600 if self.options.get('read_timeout') is None else self.options['read_timeout']

        self.row_style = self.options.get('row_style') if self.options.get('row_style') is not None else 'hash'
        if options.get('skip_startup') is not None:
            self.boot_connection()

    def reset_values(self):
        self.parameters = {}
        self.session_id = None
        self.backend_pid = None
        self.backend_key = None
        self.transaction_status = None
        self.socket = None
        self.current_job = '<initialization>'

    def on_notice(self):
        # ??
        pass

    def _socket(self):
        if self.socket is not None:
            return self.socket

        if self.options.get('ssl', False) is True:
            # SSL
            raw_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            raw_socket.connect((self.options['host'], self.options['port']))
            raw_socket.sendall(messages.SslRequest().to_bytes())
            response = raw_socket.recv(1)
            if response == 'S':
                # May want to add certs to this
                raw_socket = ssl.wrap_socket(raw_socket)
            else:
                raise SSLNotSupported("SSL requested but not supported by server")
        else:
            # Non-SSL
            raw_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            raw_socket.connect((self.options['host'], self.options['port']))

        self.socket = raw_socket
        return raw_socket

    def ssl(self):
        return self.socket is not None and isinstance(ssl.SSLSocket, self.socket)

    def opened(self):
        return self.socket is not None and self.backend_pid is not None and self.transaction_status is not None

    def closed(self):
        return not self.opened()

    def busy(self):
        return not self.ready_for_query()

    def ready_for_query(self):
        return self.current_job is None

    def write(self, message):
        if hasattr(message, 'to_bytes') is False or callable(getattr(message, 'to_bytes')) is False:
            raise TypeError("invalid message: ({0})".format(message))

        if getattr(self, 'debug', False):
            print "=> {0}".format(message)
        try:
            self._socket().sendall(message.to_bytes())
        except Exception, e:
            self.close_socket()
            raise ConnectionError(e.message)

    def close(self):
        try:
            self.write(messages.Terminate())
        finally:
            self.close_socket()

    def close_socket(self):
        try:
            self._socket().close()
            self.socket = None
        finally:
            self.reset_values()

    def reset_connection(self):
        self.close()
        self.boot_connection()

    def boot_connection(self):
        self.startup_connection()
        self.initialize_connection()

    def cancel_conn(self):
        conn = self.__class__(dict(skip_startup=True, **self.options))
        conn.write(messages.CancelRequest(self.backend_pid, self.backend_key))
        conn.write(messages.Flush())
        conn._socket().close()

    def interrupt(self):
        from vertica_python import quote
        if self.session_id is None:
            raise InterruptImpossible("Session cannot be interrupted: session ID unknown")
        conn = self.__class__(dict(interruptable=False, role=None, search_path=None, **self.options))
        response = conn.query("SELECT CLOSE_SESSION({0})".format(quote(self.session_id))).the_value()
        conn.close()
        return response

    def interruptable(self):
        self.session_id is not None

    def read_message(self):
        try:
            ready = select.select([self._socket()], [], [], self.options['read_timeout'])
            if len(ready[0]) > 0:
                type = self.read_bytes(1)
                size = unpack('!I', self.read_bytes(4))[0]

                if size < 4:
                    raise MessageError("Bad message size: {0}".format(size))
                message = BackendMessage.factory(type, self.read_bytes(size - 4))
                if getattr(self, 'debug', False):
                    print "<= {0}".format(message)
                return message
            else:
                self.close()
                raise TimedOutError("Connection timed out")
        except Exception as e:
            self.close_socket()
            raise ConnectionError(e.message)

    def process_message(self, message):
        if isinstance(message, messages.ErrorResponse):
            raise ConnectionError(message.error_message())
        elif isinstance(message, messages.NoticeResponse):
            if getattr(self, 'notice_handler', None) is not None:
                self.notice_handler(message)
        elif isinstance(message, messages.BackendKeyData):
            self.backend_pid = message.pid
            self.backend_key = message.key
        elif isinstance(message, messages.ParameterStatus):
            self.parameters[message.name] = message.value
        elif isinstance(message, messages.ReadyForQuery):
            self.transaction_status = message.transaction_status
            self.current_job = None
        else:
            raise MessageError("Unhandled message: {0}".format(message))

    # These handlers need to be fixed to support generators for non-buffered output
    def query(self, sql, options={}, handler=None):
        job = Query(self, sql, dict(row_style=self.row_style, **options))
        if handler is not None:
            job.row_handler = handler
        return self.run_with_job_lock(job)

    def copy(self, sql, source=None, handler=None):
        job = Query(self, sql, dict(row_style=self.row_style))
        if handler is not None:
            job.copy_handler = handler
        elif source is not None and os.path.isfile(str(source)):
            job.copy_handler = lambda data: self.file_copy_handler(source, data)
        elif hasattr(source, 'read'):
            job.copy_handler = lambda data: self.io_copy_handler(source, data)
        return self.run_with_job_lock(job)

    def __str__(self):
        safe_options = {}
        for key, value in self.options.iteritems():
            if key != 'password':
                safe_options[key] = value
        s1 = "<Vertica.Connection:{0} parameters={1} backend_pid={2}, ".format(id(self), self.parameters, self.backend_pid)
        s2 = "backend_key={0}, transaction_status={1}, socket={2}, options={3}, row_style={4}>".format(self.backend_key, self.transaction_status, self.socket, safe_options, self.row_style)
        return s1+s2

    def run_with_job_lock(self, job):
        if self.closed():
            self.boot_connection()
        if self.busy():
            raise SynchronizeError(self.current_job, job)
        self.current_job = job
        return job.run()

    COPY_FROM_IO_BLOCK_SIZE = 1024 * 4096

    def file_copy_handler(self, input_file, output):
        with open(input_file, 'r') as f:
            while True:
                data = f.read(self.COPY_FROM_IO_BLOCK_SIZE)
                if len(data) > 0:
                    output.write(data)
                else:
                    break

    def io_copy_handler(self, input, output):
        while True:
            data = input.read(self.COPY_FROM_IO_BLOCK_SIZE)
            if len(data) > 0:
                output.write(data)
            else:
                break

    def read_bytes(self, n):

        results = ''
        while len(results) < n:
            bytes = self._socket().recv(n-len(results))
            if bytes is None or len(bytes) == 0:
                raise ConnectionError("Connection closed by Vertica")
            results = results + bytes
        return results

    def startup_connection(self):
        self.write(messages.Startup(self.options['user'], self.options['database']))
        message = None
        while True:
            message = self.read_message()

            if isinstance(message, messages.Authentication):
                # Password message isn't right format ("incomplete message from client")
                if message.code != messages.Authentication.OK:
                    self.write(messages.Password(self.options['password'], message.code, dict(user=self.options['user'], salt=getattr(message, 'salt', None))))
            else:
                self.process_message(message)

            if isinstance(message, messages.ReadyForQuery):
                break

    def initialize_connection(self):
        if self.options.get('search_path') is not None:
            self.query("SET SEARCH_PATH TO {0}".format(self.options['search_path']))
        if self.options.get('role') is not None:
            self.query("SET ROLE {0}".format(self.options['role']))
        if self.options.get('interruptable', False) is True:
            self.session_id = self.query("SELECT session_id FROM v_monitor.current_session").the_value()
