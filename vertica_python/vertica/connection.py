# Copyright (c) 2018 Micro Focus or one of its affiliates.
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


from __future__ import print_function, division, absolute_import

import logging
import socket
import ssl
import getpass
import uuid
from struct import unpack
from collections import deque

# noinspection PyCompatibility,PyUnresolvedReferences
from builtins import str
from six import raise_from, string_types, integer_types

import vertica_python
from .. import errors
from ..vertica import messages
from ..vertica.cursor import Cursor
from ..vertica.messages.message import BackendMessage, FrontendMessage
from ..vertica.messages.frontend_messages import CancelRequest
from ..vertica.log import VerticaLogging

DEFAULT_HOST = 'localhost'
DEFAULT_USER = getpass.getuser()
DEFAULT_PORT = 5433
DEFAULT_PASSWORD = ''
DEFAULT_READ_TIMEOUT = 600
DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_PATH = 'vertica_python.log'
ASCII = 'ascii'


def connect(**kwargs):
    """Opens a new connection to a Vertica database."""
    return Connection(kwargs)


class _AddressList(object):
    def __init__(self, host, port, backup_nodes, logger):
        """Creates a new deque with the primary host first, followed by any backup hosts"""

        self._logger = logger

        # Format of items in deque: (host, port, is_dns_resolved)
        self.address_deque = deque()

        # load primary host into address_deque
        self._append(host, port)

        # load backup nodes into address_deque
        if not isinstance(backup_nodes, list):
            err_msg = 'Connection option "backup_server_node" must be a list'
            self._logger.error(err_msg)
            raise TypeError(err_msg)

        # Each item in backup_nodes should be either
        # a host name or IP address string (using default port) or
        # a (host, port) tuple
        for node in backup_nodes:
            if isinstance(node, string_types):
                self._append(node, DEFAULT_PORT)
            elif isinstance(node, tuple) and len(node) == 2:
                self._append(node[0], node[1])
            else:
                err_msg = ('Each item of connection option "backup_server_node"'
                           ' must be a host string or a (host, port) tuple')
                self._logger.error(err_msg)
                raise TypeError(err_msg)

        self._logger.debug('Address list: {0}'.format(list(self.address_deque)))

    def _append(self, host, port):
        if not isinstance(host, string_types):
            err_msg = 'Host must be a string: invalid value: {0}'.format(host)
            self._logger.error(err_msg)
            raise TypeError(err_msg)

        if not isinstance(port, (string_types, integer_types)):
            err_msg = 'Port must be an integer or a string: invalid value: {0}'.format(port)
            self._logger.error(err_msg)
            raise TypeError(err_msg)
        elif isinstance(port, string_types):
            try:
                port = int(port)
            except ValueError as e:
                err_msg = 'Port "{0}" is not a valid string: {1}'.format(port, e)
                self._logger.error(err_msg)
                raise ValueError(err_msg)

        if port < 0 or port > 65535:
            err_msg = 'Invalid port number: {0}'.format(port)
            self._logger.error(err_msg)
            raise ValueError(err_msg)

        self.address_deque.append((host, port, False))

    def push(self, host, port):
        self.address_deque.appendleft((host, port, False))

    def pop(self):
        self.address_deque.popleft()

    def peek(self):
        # do lazy DNS resolution, return the leftmost DNS-resolved address
        if len(self.address_deque) == 0:
            return None

        while len(self.address_deque) > 0:
            host, port, is_dns_resolved = self.address_deque[0]
            if is_dns_resolved:
                # return a resolved address
                self._logger.debug('Peek at address list: {0}'.format(list(self.address_deque)))
                return (host, port)
            else:
                # DNS resolve a single host name to multiple IP addresses
                self.address_deque.popleft()
                try:
                    resolved_hosts = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
                except Exception as e:
                    self._logger.warning('Error resolving host "{0}" on port {1}: {2}'.format(host, port, e))
                    continue

                # add resolved IP addresses to deque
                for res in reversed(resolved_hosts):
                    family, socktype, proto, canonname, sockaddr = res
                    self.address_deque.appendleft((sockaddr[0], sockaddr[1], True))

        return None


def _generate_session_label():
    return '{type}-{version}-{id}'.format(
        type='vertica-python',
        version=vertica_python.__version__,
        id=uuid.uuid1()
    )


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

        self.options.setdefault('host', DEFAULT_HOST)
        self.options.setdefault('port', DEFAULT_PORT)
        self.options.setdefault('user', DEFAULT_USER)
        self.options.setdefault('database', self.options['user'])
        self.options.setdefault('password', DEFAULT_PASSWORD)
        self.options.setdefault('read_timeout', DEFAULT_READ_TIMEOUT)
        self.options.setdefault('session_label', _generate_session_label())

        # Set up connection logger
        logger_name = 'vertica_{0}_{1}'.format(id(self), str(uuid.uuid4())) # must be a unique value
        self._logger = logging.getLogger(logger_name)

        if 'log_level' not in self.options and 'log_path' not in self.options:
            # logger is disabled by default
            self._logger.disabled = True
        else:
            self.options.setdefault('log_level', DEFAULT_LOG_LEVEL)
            self.options.setdefault('log_path', DEFAULT_LOG_PATH)
            VerticaLogging.setup_file_logging(logger_name, self.options['log_path'],
                                              self.options['log_level'], id(self))

        self.address_list = _AddressList(self.options['host'], self.options['port'],
                                         self.options.get('backup_server_node', []), self._logger)

        # we only support one cursor per connection
        self.options.setdefault('unicode_error', None)
        self._cursor = Cursor(self, self._logger, cursor_type=None,
                              unicode_error=self.options['unicode_error'])

        # knob for using server-side prepared statements
        self.options.setdefault('use_prepared_statements', False)
        self._logger.debug('Connection prepared statements is {}'.format(
                     'enabled' if self.options['use_prepared_statements'] else 'disabled'))

        self._logger.info('Connecting as user "{}" to database "{}" on host "{}" with port {}'.format(
                     self.options['user'], self.options['database'],
                     self.options['host'], self.options['port']))
        self.startup_connection()
        self._logger.info('Connection is ready')

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
        self.address_list = _AddressList(self.options['host'], self.options['port'],
                                         self.options.get('backup_server_node', []), self._logger)

    def _socket(self):
        if self.socket:
            return self.socket

        # the initial establishment of the client connection
        raw_socket = self.establish_connection()

        # enable load balancing
        load_balance_options = self.options.get('connection_load_balance')
        self._logger.debug('Connection load balance option is {0}'.format(
                     'enabled' if load_balance_options else 'disabled'))
        if load_balance_options:
            raw_socket = self.balance_load(raw_socket)

        # enable SSL
        ssl_options = self.options.get('ssl')
        self._logger.debug('SSL option is {0}'.format('enabled' if ssl_options else 'disabled'))
        if ssl_options:
            raw_socket = self.enable_ssl(raw_socket, ssl_options)

        self.socket = raw_socket
        return self.socket

    def create_socket(self):
        # Address family IPv6 (socket.AF_INET6) is not supported
        raw_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        raw_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        connection_timeout = self.options.get('connection_timeout')
        if connection_timeout is not None:
            self._logger.debug('Set socket connection timeout: {0}'.format(connection_timeout))
            raw_socket.settimeout(connection_timeout)
        return raw_socket

    def balance_load(self, raw_socket):
        # Send load balance request and read server response
        self._logger.debug('=> %s', messages.LoadBalanceRequest())
        raw_socket.sendall(messages.LoadBalanceRequest().get_message())
        response = raw_socket.recv(1)

        if response in (b'Y', 'Y'):
            size = unpack('!I', raw_socket.recv(4))[0]
            if size < 4:
                err_msg = "Bad message size: {0}".format(size)
                self._logger.error(err_msg)
                raise errors.MessageError(err_msg)
            res = BackendMessage.from_type(type_=response, data=raw_socket.recv(size-4))
            self._logger.debug('<= %s', res)
            host = res.get_host()
            port = res.get_port()
            self._logger.info('Load balancing to host "{0}" on port {1}'.format(host, port))

            socket_host, socket_port = raw_socket.getpeername()
            if host == socket_host and port == socket_port:
                self._logger.info('Already connecting to host "{0}" on port {1}. Ignore load balancing.'.format(host, port))
                return raw_socket

            # Push the new host onto the address list before connecting again. Note that this
            # will leave the originally-specified host as the first failover possibility.
            self.address_list.push(host, port)
            raw_socket.close()
            raw_socket = self.establish_connection()
        else:
            self._logger.debug('<= LoadBalanceResponse: %s', response)
            self._logger.warning("Load balancing requested but not supported by server")

        return raw_socket

    def enable_ssl(self, raw_socket, ssl_options):
        from ssl import CertificateError, SSLError
        # Send SSL request and read server response
        self._logger.debug('=> %s', messages.SslRequest())
        raw_socket.sendall(messages.SslRequest().get_message())
        response = raw_socket.recv(1)
        self._logger.debug('<= SslResponse: %s', response)
        if response in ('S', b'S'):
            self._logger.info('Enabling SSL')
            try:
                if isinstance(ssl_options, ssl.SSLContext):
                    host, port = raw_socket.getpeername()
                    raw_socket = ssl_options.wrap_socket(raw_socket, server_hostname=host)
                else:
                    raw_socket = ssl.wrap_socket(raw_socket)
            except CertificateError as e:
                raise_from(errors.ConnectionError, e)
            except SSLError as e:
                raise_from(errors.ConnectionError, e)
        else:
            err_msg = "SSL requested but not supported by server"
            self._logger.error(err_msg)
            raise errors.SSLNotSupported(err_msg)
        return raw_socket

    def establish_connection(self):
        addr = self.address_list.peek()
        raw_socket = None
        last_exception = None

        # Failover: loop to try all addresses
        while addr:
            last_exception = None
            host, port = addr

            self._logger.info('Establishing connection to host "{0}" on port {1}'.format(host, port))
            try:
                raw_socket = self.create_socket()
                raw_socket.connect((host, port))
                break
            except Exception as e:
                self._logger.info('Failed to connect to host "{0}" on port {1}: {2}'.format(host, port, e))
                last_exception = e
                self.address_list.pop()
                addr = self.address_list.peek()
                raw_socket.close()

        # all of the addresses failed
        if raw_socket is None or last_exception:
            err_msg = 'Failed to establish a connection to the primary server or any backup address.'
            self._logger.error(err_msg)
            raise errors.ConnectionError(err_msg)

        return raw_socket

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

        sock = self._socket()
        self._logger.debug('=> %s', message)
        try:
            for data in message.fetch_message():
                try:
                    sock.sendall(data)
                except Exception:
                    self._logger.error("couldn't send message")
                    raise

        except Exception as e:
            self.close_socket()
            self._logger.error(str(e))
            raise

    def close_socket(self):
        try:
            if self.socket is not None:
                self._socket().close()
        finally:
            self.reset_values()

    def reset_connection(self):
        self.close()
        self.startup_connection()

    def is_asynchronous_message(self, message):
        # Check if it is an asynchronous response message
        # Note: ErrorResponse is a subclass of NoticeResponse
        return (isinstance(message, messages.ParameterStatus) or
            (isinstance(message, messages.NoticeResponse) and
             not isinstance(message, messages.ErrorResponse)))

    def handle_asynchronous_message(self, message):
        if isinstance(message, messages.ParameterStatus):
            self.parameters[message.name] = message.value
        elif (isinstance(message, messages.NoticeResponse) and
             not isinstance(message, messages.ErrorResponse)):
            if getattr(self, 'notice_handler', None) is not None:
                self.notice_handler(message)
            else:
                self._logger.warning(message.error_message())

    def read_message(self):
        while True:
            try:
                type_ = self.read_bytes(1)
                size = unpack('!I', self.read_bytes(4))[0]
                if size < 4:
                    raise errors.MessageError("Bad message size: {0}".format(size))
                message = BackendMessage.from_type(type_, self.read_bytes(size - 4))
                self._logger.debug('<= %s', message)
                self.handle_asynchronous_message(message)
            except (SystemError, IOError) as e:
                self.close_socket()
                # noinspection PyTypeChecker
                self._logger.error(e)
                raise_from(errors.ConnectionError, e)
            if not self.is_asynchronous_message(message):
                break
        return message

    def read_expected_message(self, expected_types, error_handler=None):
        # Reads a message and does some basic error handling.
        # expected_types must be a class (e.g. messages.BindComplete) or a tuple of classes
        message = self.read_message()
        if isinstance(message, expected_types):
            return message
        elif isinstance(message, messages.ErrorResponse):
            if error_handler is not None:
                error_handler(message)
            else:
                raise errors.DatabaseError(message.error_message())
        else:
            msg = 'Received unexpected message type: {}. '.format(type(message).__name__)
            if isinstance(expected_types, tuple):
                msg += 'Expected types: {}'.format(", ".join([t.__name__ for t in expected_types]))
            else:
                msg += 'Expected type: {}'.format(expected_types.__name__)
            self._logger.error(msg)
            raise errors.MessageError(msg)

    def process_message(self, message):
        if isinstance(message, messages.ErrorResponse):
            raise errors.ConnectionError(message.error_message())
        elif isinstance(message, messages.BackendKeyData):
            self.backend_pid = message.pid
            self.backend_key = message.key
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
        session_label = self.options['session_label'].encode(ASCII)

        self.write(messages.Startup(user, database, session_label))

        while True:
            message = self.read_message()

            if isinstance(message, messages.Authentication):
                # Password message isn't right format ("incomplete message from client")
                if message.code == messages.Authentication.OK:
                    self._logger.info("User {} successfully authenticated"
                        .format(self.options['user']))
                elif message.code == messages.Authentication.CHANGE_PASSWORD:
                    msg = "The password for user {} has expired".format(self.options['user'])
                    self._logger.error(msg)
                    raise errors.ConnectionError(msg)
                elif message.code == messages.Authentication.PASSWORD_GRACE:
                    self._logger.warning('The password for user {} will expire soon.'
                        ' Please consider changing it.'.format(self.options['user']))
                else:
                    self.write(messages.Password(password, message.code,
                                                 {'user': user,
                                                  'salt': getattr(message, 'salt', None),
                                                  'usersalt': getattr(message, 'usersalt', None)}))
            else:
                self.process_message(message)

            if isinstance(message, messages.ReadyForQuery):
                break
