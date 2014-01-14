from __future__ import absolute_import

import os
import select
import socket
import ssl

from struct import unpack

import vertica_python.errors as errors
import vertica_python.vertica.messages as messages

from vertica_python.vertica.messages.message import BackendMessage

from vertica_python.vertica.cursor  import Cursor

# To support vertica_python 0.1.9 interface
class OldResults(object):
    def __init__(self, rows):
        self.rows = rows

class Connection(object):

    def __init__(self, options={}):
        self.reset_values()

        self.options = {}

        for key, value in options.iteritems():
            if value is not None:
                self.options[key] = value

        self.options['port'] = 5433 if self.options.get('port') is None else self.options['port']
        self.options['read_timeout'] = 600 if self.options.get('read_timeout') is None else self.options['read_timeout']

        self.row_style = self.options.get('row_style') if self.options.get('row_style') is not None else 'hash'
        self.boot_connection()
        #self.debug = True


    #
    # To support vertica_python 0.1.9 interface
    #
    def query(self, query, handler=None):
        if handler:
            cur = Cursor(self, 'dict', handler)
            cur.execute(query)
        else:
            cur = Cursor(self, 'dict')
            cur.execute(query)
            return OldResults(cur.fetchall())

    #
    # dbApi methods
    #

    def close(self):
        try:
            self.write(messages.Terminate())
        finally:
            self.close_socket()

    def commit(self):
        if self.closed():
            raise errors.Error('Connection is closed')

        cur = self.cursor()
        cur.execute('commit')

    def rollback(self):
        if self.closed():
            raise errors.Error('Connection is closed')

        cur = self.cursor()
        cur.execute('rollback')

    def cursor(self, cursor_type=None, row_handler=None):
        if self.closed():
            raise errors.Error('Connection is closed')
        return Cursor(self, cursor_type=cursor_type, row_handler=row_handler)



    #
    # Internal 
    #

    def reset_values(self):
        self.parameters = {}
        self.session_id = None
        self.backend_pid = None
        self.backend_key = None
        self.transaction_status = None
        self.socket = None

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

    def write(self, message):

        if hasattr(message, 'to_bytes') is False or callable(getattr(message, 'to_bytes')) is False:
            raise TypeError("invalid message: ({0})".format(message))

        if getattr(self, 'debug', False):
            print "=> {0}".format(message)
        try:
            self._socket().sendall(message.to_bytes())
        except Exception, e:
            self.close_socket()
            raise errors.ConnectionError(e.message)

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

    def read_message(self):
        try:
            ready = select.select([self._socket()], [], [], self.options['read_timeout'])
            if len(ready[0]) > 0:
                type = self.read_bytes(1)
                size = unpack('!I', self.read_bytes(4))[0]

                if size < 4:
                    raise errors.MessageError("Bad message size: {0}".format(size))
                message = BackendMessage.factory(type, self.read_bytes(size - 4))
                if getattr(self, 'debug', False):
                    print "<= {0}".format(message)
                return message
            else:
                self.close()
                raise errors.TimedOutError("Connection timed out")
        except Exception as e:
            self.close_socket()
            raise errors.ConnectionError(e.message)

    def process_message(self, message):
        if isinstance(message, messages.ErrorResponse):
            raise errors.ConnectionError(message.error_message())
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
        else:
            raise errors.MessageError("Unhandled message: {0}".format(message))

    def __str__(self):
        safe_options = {}
        for key, value in self.options.iteritems():
            if key != 'password':
                safe_options[key] = value
        s1 = "<Vertica.Connection:{0} parameters={1} backend_pid={2}, ".format(id(self), self.parameters, self.backend_pid)
        s2 = "backend_key={0}, transaction_status={1}, socket={2}, options={3}, row_style={4}>".format(self.backend_key, self.transaction_status, self.socket, safe_options, self.row_style)
        return s1+s2



    def read_bytes(self, n):
        results = ''
        while len(results) < n:
            bytes = self._socket().recv(n-len(results))
            if bytes is None or len(bytes) == 0:
                raise errors.ConnectionError("Connection closed by Vertica")
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
        #if self.options.get('interruptable', False) is True:
        #    self.session_id = self.query("SELECT session_id FROM v_monitor.current_session").the_value()


