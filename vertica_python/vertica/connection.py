from __future__ import print_function, division, absolute_import

import logging
import socket
import ssl
from struct import unpack

# noinspection PyCompatibility,PyUnresolvedReferences
from builtins import str
from six import raise_from

from .. import errors
from ..vertica import messages
from ..vertica.cursor import Cursor
from ..vertica.messages.message import BackendMessage, FrontendMessage
from ..vertica.messages.frontend_messages import CancelRequest

logger = logging.getLogger('vertica')

ASCII = 'ascii'


def connect(**kwargs):
    """Opens a new connection to a Vertica database."""
    return Connection(kwargs)


class Connection(object):
    def __init__(self, options=None):
        self.parameters = {}
        self.session_id = None
        self.backend_pid = None
        self.backend_key = None
        self.transaction_status = None
        self.socket = None

        options = options or {}
        self.options = {key: value for key, value in options.items() if value is not None}

        # we only support one cursor per connection
        self.options.setdefault('unicode_error', None)
        self._cursor = Cursor(self, None, unicode_error=self.options['unicode_error'])
        self.options.setdefault('port', 5433)
        self.options.setdefault('read_timeout', 600)
        self.startup_connection()

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        try:
            # if there's no outstanding transaction, we can simply close the connection
            if self.transaction_status in (None, 'in_transaction'):
                return

            if type_ is not None:
                self.rollback()
            else:
                self.commit()
        finally:
            self.close()

    #############################################
    # dbapi methods
    #############################################
    def close(self):
        try:
            self.write(messages.Terminate())
        finally:
            self.close_socket()

    def cancel(self):
        if self.closed():
            raise errors.ConnectionError('Connection is closed')

        self.write(CancelRequest(backend_pid=self.backend_pid, backend_key=self.backend_key))

    def commit(self):
        if self.closed():
            raise errors.ConnectionError('Connection is closed')

        cur = self.cursor()
        cur.execute('COMMIT;')

    def rollback(self):
        if self.closed():
            raise errors.ConnectionError('Connection is closed')

        cur = self.cursor()
        cur.execute('ROLLBACK;')

    def cursor(self, cursor_type=None):
        if self.closed():
            raise errors.ConnectionError('Connection is closed')

        if self._cursor.closed():
            self._cursor._closed = False

        # let user change type if they want?
        self._cursor.cursor_type = cursor_type
        return self._cursor

    #############################################
    # internal
    #############################################
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

        host = self.options.get('host')
        port = self.options.get('port')
        connection_timeout = self.options.get('connection_timeout')
        raw_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        raw_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if connection_timeout is not None:
            raw_socket.settimeout(connection_timeout)
        raw_socket.connect((host, port))

        ssl_options = self.options.get('ssl')
        if ssl_options is not None and ssl_options is not False:
            from ssl import CertificateError, SSLError
            raw_socket.sendall(messages.SslRequest().get_message())
            response = raw_socket.recv(1)
            if response in ('S', b'S'):
                try:
                    if isinstance(ssl_options, ssl.SSLContext):
                        raw_socket = ssl_options.wrap_socket(raw_socket, server_hostname=host)
                    else:
                        raw_socket = ssl.wrap_socket(raw_socket)
                except CertificateError as e:
                    raise_from(errors.ConnectionError, e)
                except SSLError as e:
                    raise_from(errors.ConnectionError, e)
            else:
                raise errors.SSLNotSupported("SSL requested but not supported by server")

        self.socket = raw_socket
        return self.socket

    def ssl(self):
        return self.socket is not None and isinstance(self.socket, ssl.SSLSocket)

    def opened(self):
        return (self.socket is not None
                and self.backend_pid is not None
                and self.transaction_status is not None)

    def closed(self):
        return not self.opened()

    def write(self, message):
        if not isinstance(message, FrontendMessage):
            raise TypeError("invalid message: ({0})".format(message))

        logger.debug('=> %s', message)

        try:
            for data in message.fetch_message():
                try:
                    self._socket().sendall(data)
                except Exception:
                    logger.error("couldn't send message")
                    raise

        except Exception as e:
            self.close_socket()
            if str(e) == 'unsupported authentication method: 9':
                raise errors.ConnectionError(
                    'Error during authentication. Your password might be expired.')
            else:
                # noinspection PyTypeChecker
                raise_from(errors.ConnectionError, e)

    def close_socket(self):
        try:
            if self.socket is not None:
                self._socket().close()
        finally:
            self.reset_values()

    def reset_connection(self):
        self.close()
        self.startup_connection()

    def read_message(self):
        try:
            type_ = self.read_bytes(1)
            size = unpack('!I', self.read_bytes(4))[0]

            if size < 4:
                raise errors.MessageError("Bad message size: {0}".format(size))
            message = BackendMessage.from_type(type_, self.read_bytes(size - 4))
            logger.debug('<= %s', message)
            return message
        except (SystemError, IOError) as e:
            self.close_socket()
            # noinspection PyTypeChecker
            raise_from(errors.ConnectionError, e)

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
        elif isinstance(message, messages.CommandComplete):
            # TODO: I'm not ever seeing this actually returned by vertica...
            # if vertica returns a row count, set the rowcount attribute in cursor
            # if hasattr(message, 'rows'):
            #     self.cursor.rowcount = message.rows
            pass
        elif isinstance(message, messages.EmptyQueryResponse):
            pass
        elif isinstance(message, messages.CopyInResponse):
            pass
        else:
            raise errors.MessageError("Unhandled message: {0}".format(message))

        # set last message
        self._cursor._message = message

    def __str__(self):
        safe_options = {key: value for key, value in self.options.items() if key != 'password'}

        s1 = "<Vertica.Connection:{0} parameters={1} backend_pid={2}, ".format(
            id(self), self.parameters, self.backend_pid)
        s2 = "backend_key={0}, transaction_status={1}, socket={2}, options={3}>".format(
            self.backend_key, self.transaction_status, self.socket, safe_options)
        return ''.join([s1, s2])

    def read_bytes(self, n):
        results = bytes()
        while len(results) < n:
            bytes_ = self._socket().recv(n - len(results))
            if not bytes_:
                raise errors.ConnectionError("Connection closed by Vertica")
            results += bytes_
        return results

    def startup_connection(self):
        # This doesn't handle Unicode usernames or passwords
        user = self.options['user'].encode(ASCII)
        database = self.options['database'].encode(ASCII)
        password = self.options['password'].encode(ASCII)

        self.write(messages.Startup(user, database))

        while True:
            message = self.read_message()

            if isinstance(message, messages.Authentication):
                # Password message isn't right format ("incomplete message from client")
                if message.code != messages.Authentication.OK:
                    self.write(messages.Password(password, message.code,
                                                 {'user': user,
                                                  'salt': getattr(message, 'salt', None)}))
            else:
                self.process_message(message)

            if isinstance(message, messages.ReadyForQuery):
                break
