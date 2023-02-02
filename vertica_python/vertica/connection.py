# Copyright (c) 2018-2023 Micro Focus or one of its affiliates.
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

import base64
import logging
import socket
import ssl
import getpass
import uuid
from struct import unpack
from collections import deque, namedtuple
import random

# noinspection PyCompatibility,PyUnresolvedReferences
from urllib.parse import urlparse, parse_qs
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from typing import Any, Dict, Literal, Optional, Type, Union
    from typing_extensions import Self

import vertica_python
from .. import errors
from ..vertica import messages
from ..vertica.cursor import Cursor
from ..vertica.messages.message import BackendMessage, FrontendMessage
from ..vertica.messages.frontend_messages import CancelRequest
from ..vertica.log import VerticaLogging

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 5433
DEFAULT_PASSWORD = ''
DEFAULT_DATABASE = ''
DEFAULT_AUTOCOMMIT = False
DEFAULT_BACKUP_SERVER_NODE = []
DEFAULT_KRB_SERVICE_NAME = 'vertica'
DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_PATH = 'vertica_python.log'
DEFAULT_BINARY_TRANSFER = False
DEFAULT_REQUEST_COMPLEX_TYPES = True
try:
    DEFAULT_USER = getpass.getuser()
except Exception as e:
    DEFAULT_USER = None
    print("WARN: Cannot get the login user name: {}".format(str(e)))


def connect(**kwargs):
    # type: (Any) -> Connection
    """Opens a new connection to a Vertica database."""
    return Connection(kwargs)


def parse_dsn(dsn):
    """Parse connection string into a dictionary of keywords and values.
       Connection string format:
           vertica://<user>:<password>@<host>:<port>/<database>?k1=v1&k2=v2&...
    """
    url = urlparse(dsn)
    if url.scheme != 'vertica':
        raise ValueError("Only vertica:// scheme is supported.")

    # Ignore blank/invalid values
    result = {k: v for k, v in (
        ('host', url.hostname),
        ('port', url.port),
        ('user', url.username),
        ('password', url.password),
        ('database', url.path[1:])) if v
    }
    for key, values in parse_qs(url.query, keep_blank_values=True).items():
        # Try to get the last non-blank value in the list of values for each key
        for i in reversed(range(len(values))):
            value = values[i]
            if value != '':
                break

        if value == '' and key != 'log_path':
            # blank values are to be ignored
            continue
        elif key == 'backup_server_node':
            continue
        elif key in ('connection_load_balance', 'use_prepared_statements',
                     'disable_copy_local', 'ssl', 'autocommit',
                     'binary_transfer', 'request_complex_types'):
            lower = value.lower()
            if lower in ('true', 'on', '1'):
                result[key] = True
            elif lower in ('false', 'off', '0'):
                result[key] = False
        elif key == 'connection_timeout':
            result[key] = float(value)
        elif key == 'log_level' and value.isdigit():
            result[key] = int(value)
        else:
            result[key] = value

    return result

_AddressEntry = namedtuple('_AddressEntry', ['host', 'resolved', 'data'])

class _AddressList(object):
    def __init__(self, host, port, backup_nodes, logger):
        """Creates a new deque with the primary host first, followed by any backup hosts"""

        self._logger = logger

        # Items in address_deque are _AddressEntry values.
        #   host is the original hostname/ip, used by SSL option check_hostname
        #   - when resolved is False, data is port
        #   - when resolved is True, data is the 5-tuple from socket.getaddrinfo
        # This allows for lazy resolution. Seek peek() for more.
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
            if isinstance(node, str):
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
        if not isinstance(host, str):
            err_msg = 'Host must be a string: invalid value: {0}'.format(host)
            self._logger.error(err_msg)
            raise TypeError(err_msg)

        if not isinstance(port, (str, int)):
            err_msg = 'Port must be an integer or a string: invalid value: {0}'.format(port)
            self._logger.error(err_msg)
            raise TypeError(err_msg)
        elif isinstance(port, str):
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

        self.address_deque.append(_AddressEntry(host=host, resolved=False, data=port))

    def push(self, host, port):
        self.address_deque.appendleft(_AddressEntry(host=host, resolved=False, data=port))

    def pop(self):
        self.address_deque.popleft()

    def peek(self):
        # do lazy DNS resolution, returning the leftmost socket.getaddrinfo result
        if len(self.address_deque) == 0:
            return None

        while len(self.address_deque) > 0:
            self._logger.debug('Peek at address list: {0}'.format(list(self.address_deque)))
            entry = self.address_deque[0]
            if entry.resolved:
                # return a resolved sockaddrinfo
                return entry.data
            else:
                # DNS resolve a single host name to multiple IP addresses
                self.pop()
                # keep host and port info for adding address entry to deque once it has been resolved
                host, port = entry.host, entry.data
                try:
                    resolved_hosts = socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM)
                except Exception as e:
                    self._logger.warning('Error resolving host "{0}" on port {1}: {2}'.format(host, port, e))
                    continue

                # add resolved addrinfo (AF_INET and AF_INET6 only) to deque
                random.shuffle(resolved_hosts)
                for addrinfo in resolved_hosts:
                    if addrinfo[0] in (socket.AF_INET, socket.AF_INET6):
                        self.address_deque.appendleft(_AddressEntry(
                            host=host, resolved=True, data=addrinfo))
        return None

    def peek_host(self):
        # returning the leftmost host result
        self._logger.debug('Peek host at address list: {0}'.format(list(self.address_deque)))
        if len(self.address_deque) == 0:
            return None
        return self.address_deque[0].host


def _generate_session_label():
    return '{type}-{version}-{id}'.format(
        type='vertica-python',
        version=vertica_python.__version__,
        id=uuid.uuid1()
    )


class Connection(object):
    def __init__(self, options=None):
        # type: (Optional[Dict[str, Any]]) -> None
        self.parameters = {}
        self.session_id = None
        self.backend_pid = None
        self.backend_key = None
        self.transaction_status = None
        self.socket = None
        self.socket_as_file = None

        options = options or {}
        self.options = parse_dsn(options['dsn']) if 'dsn' in options else {}
        self.options.update({key: value for key, value in options.items() \
                             if key == 'log_path' or (key != 'dsn' and value is not None)})

        # Set up connection logger
        logger_name = 'vertica_{0}_{1}'.format(id(self), str(uuid.uuid4())) # must be a unique value
        self._logger = logging.getLogger(logger_name)

        if 'log_level' not in self.options and 'log_path' not in self.options:
            # logger is disabled by default
            self._logger.disabled = True
        else:
            self.options.setdefault('log_level', DEFAULT_LOG_LEVEL)
            self.options.setdefault('log_path', DEFAULT_LOG_PATH)
            VerticaLogging.setup_logging(logger_name, self.options['log_path'],
                                         self.options['log_level'], id(self))

        self.options.setdefault('host', DEFAULT_HOST)
        self.options.setdefault('port', DEFAULT_PORT)
        if 'user' not in self.options:
            if DEFAULT_USER:
                self.options['user'] = DEFAULT_USER
            else:
                msg = 'Connection option "user" is required'
                self._logger.error(msg)
                raise KeyError(msg)
        self.options.setdefault('database', DEFAULT_DATABASE)
        self.options.setdefault('password', DEFAULT_PASSWORD)
        self.options.setdefault('autocommit', DEFAULT_AUTOCOMMIT)
        self.options.setdefault('session_label', _generate_session_label())
        self.options.setdefault('backup_server_node', DEFAULT_BACKUP_SERVER_NODE)
        self.options.setdefault('kerberos_service_name', DEFAULT_KRB_SERVICE_NAME)
        # Kerberos authentication hostname defaults to the host value here so
        # the correct value cannot be overwritten by load balancing or failover
        self.options.setdefault('kerberos_host_name', self.options['host'])

        self.address_list = _AddressList(self.options['host'], self.options['port'],
                                         self.options['backup_server_node'], self._logger)

        # we only support one cursor per connection
        self.options.setdefault('unicode_error', None)
        self._cursor = Cursor(self, self._logger, cursor_type=None,
                              unicode_error=self.options['unicode_error'])

        # knob for using server-side prepared statements
        self.options.setdefault('use_prepared_statements', False)
        self._logger.debug('Connection prepared statements is {}'.format(
                     'enabled' if self.options['use_prepared_statements'] else 'disabled'))

        # knob for disabling COPY LOCAL operations
        self.options.setdefault('disable_copy_local', False)
        self._logger.debug('COPY LOCAL operation is {}'.format(
                     'disabled' if self.options['disable_copy_local'] else 'enabled'))

        # knob for using binary transfer format or text transfer format
        self.options.setdefault('binary_transfer', DEFAULT_BINARY_TRANSFER)
        self._logger.debug('Data binary transfer is {}'.format(
                     'enabled' if self.options['binary_transfer'] else 'disabled'))

        # knob for requesting complex types metadata
        self.options.setdefault('request_complex_types', DEFAULT_REQUEST_COMPLEX_TYPES)
        self._logger.debug('Complex types metadata is {}'.format(
                     'requested' if self.options['request_complex_types'] else 'not requested'))

        self._logger.info('Connecting as user "{}" to database "{}" on host "{}" with port {}'.format(
                     self.options['user'], self.options['database'],
                     self.options['host'], self.options['port']))
        self.startup_connection()

        # Complex types metadata is returned since protocol version 3.12
        self.complex_types_enabled = self.parameters['protocol_version'] >= (3 << 16 | 12) and \
                                     self.parameters.get('request_complex_types', 'off') == 'on'
        self._logger.info('Connection is ready')

    #############################################
    # supporting `with` statements
    #############################################
    def __enter__(self):
        # type: () -> Self
        return self

    def __exit__(self, type_, value, traceback):
        self.close()

    #############################################
    # dbapi methods
    #############################################
    def close(self):
        self._logger.info('Close the connection')
        try:
            self.write(messages.Terminate())
        finally:
            self.close_socket()

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
        # type: (Self, Optional[Union[Literal['list', 'dict'], Type[list[Any]], Type[dict[Any, Any]]]]) -> Cursor
        if self.closed():
            raise errors.ConnectionError('Connection is closed')

        if self._cursor.closed():
            self._cursor._closed = False

        # let user change type if they want?
        self._cursor.cursor_type = cursor_type
        return self._cursor

    #############################################
    # non-dbapi methods
    #############################################
    @property
    def autocommit(self):
        """Read the connection's AUTOCOMMIT setting from cache"""
        # For a new session, autocommit is off by default
        return self.parameters.get('auto_commit', 'off') == 'on'

    @autocommit.setter
    def autocommit(self, value):
        """Change the connection's AUTOCOMMIT setting"""
        if self.autocommit is value:
            return
        val = 'on' if value else 'off'
        cur = self.cursor()
        cur.execute('SET SESSION AUTOCOMMIT TO {}'.format(val), use_prepared_statements=False)
        cur.fetchall()   # check for errors and update the cache

    def cancel(self):
        """Cancel the current database operation. This can be called from a
           different thread than the one currently executing a database operation.
        """
        if self.closed():
            raise errors.ConnectionError('Connection is closed')
        self._logger.info('Canceling the current database operation')
        # Must create a new socket connection to the server
        temp_socket = self.establish_socket_connection(self.address_list)
        self.write(CancelRequest(self.backend_pid, self.backend_key), temp_socket)
        temp_socket.close()

        self._logger.info('Cancel request issued')

    def opened(self):
        return (self.socket is not None
                and self.backend_pid is not None
                and self.transaction_status is not None)

    def closed(self):
        return not self.opened()

    def __str__(self):
        safe_options = {key: value for key, value in self.options.items() if key != 'password'}

        s1 = "<Vertica.Connection:{0} parameters={1} backend_pid={2}, ".format(
            id(self), self.parameters, self.backend_pid)
        s2 = "backend_key={0}, transaction_status={1}, socket={2}, options={3}>".format(
            self.backend_key, self.transaction_status, self.socket, safe_options)
        return ''.join([s1, s2])

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
        self.socket_as_file = None
        self.address_list = _AddressList(self.options['host'], self.options['port'],
                                         self.options['backup_server_node'], self._logger)

    def _socket(self):
        if self.socket:
            return self.socket

        # the initial establishment of the socket connection
        raw_socket = self.establish_socket_connection(self.address_list)

        # modify the socket connection based on client connection options
        try:
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
        except:
            self._logger.debug('Close the socket')
            raw_socket.close()
            raise

        self.socket = raw_socket
        return self.socket

    def _socket_as_file(self):
        if self.socket_as_file is None:
            self.socket_as_file = self._socket().makefile('rb')
        return self.socket_as_file

    def create_socket(self, family):
        """Create a TCP socket object"""
        raw_socket = socket.socket(family, socket.SOCK_STREAM)
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

        if response == b'Y':
            size = unpack('!I', raw_socket.recv(4))[0]
            if size < 4:
                err_msg = "Bad message size: {0}".format(size)
                self._logger.error(err_msg)
                raise errors.MessageError(err_msg)
            res = BackendMessage.from_type(type_=response, data=raw_socket.recv(size - 4))
            self._logger.debug('<= %s', res)
            host = res.get_host()
            port = res.get_port()
            self._logger.info('Load balancing to host "{0}" on port {1}'.format(host, port))

            peer = raw_socket.getpeername()
            socket_host, socket_port = peer[0], peer[1]
            if host == socket_host and port == socket_port:
                self._logger.info('Already connecting to host "{0}" on port {1}. Ignore load balancing.'.format(host, port))
                return raw_socket

            # Push the new host onto the address list before connecting again. Note that this
            # will leave the originally-specified host as the first failover possibility.
            self.address_list.push(host, port)
            raw_socket.close()
            raw_socket = self.establish_socket_connection(self.address_list)
        else:
            self._logger.debug('<= LoadBalanceResponse: %s', response)
            self._logger.warning("Load balancing requested but not supported by server")

        return raw_socket

    def enable_ssl(self, raw_socket, ssl_options):
        # Send SSL request and read server response
        self._logger.debug('=> %s', messages.SslRequest())
        raw_socket.sendall(messages.SslRequest().get_message())
        response = raw_socket.recv(1)
        self._logger.debug('<= SslResponse: %s', response)
        if response == b'S':
            self._logger.info('Enabling SSL')
            try:
                if isinstance(ssl_options, ssl.SSLContext):
                    server_host = self.address_list.peek_host()
                    if server_host is None:   # This should not happen
                        msg = 'Cannot get the connected server host while enabling SSL'
                        self._logger.error(msg)
                        raise errors.ConnectionError(msg)
                    raw_socket = ssl_options.wrap_socket(raw_socket, server_hostname=server_host)
                else:
                    raw_socket = ssl.wrap_socket(raw_socket)
            except ssl.CertificateError as e:
                raise errors.ConnectionError(str(e))
            except ssl.SSLError as e:
                raise errors.ConnectionError(str(e))
        else:
            err_msg = "SSL requested but not supported by server"
            self._logger.error(err_msg)
            raise errors.SSLNotSupported(err_msg)
        return raw_socket

    def establish_socket_connection(self, address_list):
        """Given a list of database node addresses, establish the socket
           connection to the database server. Return a connected socket object.
        """
        addrinfo = address_list.peek()
        raw_socket = None
        last_exception = None

        # Failover: loop to try all addresses
        while addrinfo:
            (family, socktype, proto, canonname, sockaddr) = addrinfo
            last_exception = None

            # _AddressList filters all addrs to AF_INET and AF_INET6, which both
            # have host and port as values 0, 1 in the sockaddr tuple.
            host = sockaddr[0]
            port = sockaddr[1]

            self._logger.info('Establishing connection to host "{0}" on port {1}'.format(host, port))

            try:
                raw_socket = self.create_socket(family)
                raw_socket.connect(sockaddr)
                break
            except Exception as e:
                self._logger.info('Failed to connect to host "{0}" on port {1}: {2}'.format(host, port, e))
                last_exception = e
                address_list.pop()
                addrinfo = address_list.peek()
                raw_socket.close()

        # all of the addresses failed
        if raw_socket is None or last_exception:
            err_msg = 'Failed to establish a connection to the primary server or any backup address.'
            self._logger.error(err_msg)
            raise errors.ConnectionError(err_msg)

        return raw_socket

    def ssl(self):
        return self.socket is not None and isinstance(self.socket, ssl.SSLSocket)

    def write(self, message, vsocket=None):
        if not isinstance(message, FrontendMessage):
            raise TypeError("invalid message: ({0})".format(message))
        if vsocket is None:
            vsocket = self._socket()
        self._logger.debug('=> %s', message)
        try:
            for data in message.fetch_message():
                size = 8192 # Max msg size, consistent with how the server works
                pos = 0
                while pos < len(data):
                    sent = vsocket.send(data[pos : pos + size])
                    if sent == 0:
                        raise errors.ConnectionError("Couldn't send message: Socket connection broken")
                    pos += sent
        except Exception as e:
            self.close_socket()
            self._logger.error(str(e))
            if isinstance(e, IOError):
                raise errors.ConnectionError(str(e))
            else:
                raise

    def close_socket(self):
        self._logger.debug("Close connection's socket")
        try:
            if self.socket is not None:
                self._socket().close()
            if self.socket_as_file is not None:
                self._socket_as_file().close()
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
            if message.name == 'protocol_version':
                message.value = int(message.value)
            self.parameters[message.name] = message.value
        elif (isinstance(message, messages.NoticeResponse) and
             not isinstance(message, messages.ErrorResponse)):
            if getattr(self, 'notice_handler', None) is not None:
                self.notice_handler(message)
            else:
                self._logger.warning(message.error_message())

    def read_string(self):
        s = bytearray()
        while True:
            char = self.read_bytes(1)
            if char == b'\x00':
                break
            s.extend(char)
        return s

    def read_message(self):
        while True:
            try:
                type_ = self.read_bytes(1)
                size = unpack('!I', self.read_bytes(4))[0]
                if size < 4:
                    raise errors.MessageError("Bad message size: {0}".format(size))
                if type_ == messages.WriteFile.message_id:
                    # The whole WriteFile message may not be read at here.
                    # Instead, only the file name and file length is read.
                    # This is because the message could be too large to read all at once.
                    f = self.read_string()
                    filename = f.decode('utf-8')
                    file_length = unpack('!I', self.read_bytes(4))[0]
                    size -= 4 + len(f) + 1 + 4
                    if size != file_length:
                        raise errors.MessageError("Bad message size: {0}".format(size))
                    if filename == '':
                        # If there is no filename, then this is really RETURNREJECTED data, not a rejected file
                        if file_length % 8 != 0:
                            raise errors.MessageError("Bad RETURNREJECTED data size: {0}".format(file_length))
                        data = self.read_bytes(file_length)
                        message = messages.WriteFile(filename, file_length, data)
                    else:
                        # The rest of the message is read later with write_to_disk()
                        message = messages.WriteFile(filename, file_length)
                elif type_ == messages.RowDescription.message_id:
                    message = BackendMessage.from_type(type_, self.read_bytes(size - 4), complex_types_enabled=self.complex_types_enabled)
                else:
                    message = BackendMessage.from_type(type_, self.read_bytes(size - 4))
                self._logger.debug('<= %s', message)
                self.handle_asynchronous_message(message)
                # handle transaction status
                if isinstance(message, messages.ReadyForQuery):
                    self.transaction_status = message.transaction_status
            except (SystemError, IOError) as e:
                self.close_socket()
                # noinspection PyTypeChecker
                self._logger.error(e)
                raise errors.ConnectionError(str(e))
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

    def read_bytes(self, n):
        if n == 1:
            result = self._socket_as_file().read(1)
            if not result:
                raise errors.ConnectionError("Connection closed by Vertica")
            return result
        else:
            buf = b""
            to_read = n
            while to_read > 0:
                data = self._socket_as_file().read(to_read)
                received = len(data)
                if received == 0:
                    raise errors.ConnectionError("Connection closed by Vertica")
                buf += data
                to_read -= received
            return buf

    def send_GSS_response_and_receive_challenge(self, response):       
        # Send the GSS response data to the vertica server
        token = base64.b64decode(response)
        self.write(messages.Password(token, messages.Authentication.GSS))
        # Receive the challenge from the vertica server
        message = self.read_expected_message(messages.Authentication)
        if message.code != messages.Authentication.GSS_CONTINUE:
            msg = ('Received unexpected message type: Authentication(type={}).'
                   ' Expected type: Authentication(type={})'.format(
                   message.code, messages.Authentication.GSS_CONTINUE))
            self._logger.error(msg)
            raise errors.MessageError(msg)
        return message.auth_data

    def make_GSS_authentication(self):
        try:
            import kerberos
        except ImportError as e:
            raise errors.ConnectionError("{}\nCannot make a Kerberos "
                "authentication because no Kerberos package is installed. "
                "Get it with 'pip install kerberos'.".format(str(e)))

        # Set GSS flags
        gssflag = (kerberos.GSS_C_DELEG_FLAG | kerberos.GSS_C_MUTUAL_FLAG |
                   kerberos.GSS_C_SEQUENCE_FLAG | kerberos.GSS_C_REPLAY_FLAG)

        # Generate the GSS-style service principal name
        service_principal = "{}@{}".format(self.options['kerberos_service_name'],
                                           self.options['kerberos_host_name'])

        # Initializes a context object with a service principal
        self._logger.info('Initializing a context for GSSAPI client-side '
            'authentication with service principal {}'.format(service_principal))
        try:
            result, context = kerberos.authGSSClientInit(service_principal, gssflags=gssflag)
        except kerberos.GSSError as err:
            msg = "GSSAPI initialization error: {}".format(str(err))
            self._logger.error(msg)
            raise errors.KerberosError(msg)
        if result != kerberos.AUTH_GSS_COMPLETE:
            msg = ('Failed to initialize a context for GSSAPI client-side '
                   'authentication with service principal {}'.format(service_principal))
            self._logger.error(msg)
            raise errors.KerberosError(msg)

        # Processes GSSAPI client-side steps
        try:
            challenge = b''
            while True:
                self._logger.info('Processing a single GSSAPI client-side step')
                challenge = base64.b64encode(challenge).decode("utf-8")
                result = kerberos.authGSSClientStep(context, challenge)

                if result == kerberos.AUTH_GSS_COMPLETE:
                    self._logger.info('Result: GSSAPI step complete')
                    break
                elif result == kerberos.AUTH_GSS_CONTINUE:
                    self._logger.info('Result: GSSAPI step continuation')
                    # Get the response from the last successful GSSAPI client-side step
                    response = kerberos.authGSSClientResponse(context)
                    challenge = self.send_GSS_response_and_receive_challenge(response)
                else:
                    msg = "GSSAPI client-side step error status {}".format(result)
                    self._logger.error(msg)
                    raise errors.KerberosError(msg)
        except kerberos.GSSError as err:
            msg = "GSSAPI client-side step error: {}".format(str(err))
            self._logger.error(msg)
            raise errors.KerberosError(msg)

    def startup_connection(self):
        user = self.options['user']
        database = self.options['database']
        session_label = self.options['session_label']
        os_user_name = DEFAULT_USER if DEFAULT_USER else ''
        password = self.options['password']
        autocommit = self.options['autocommit']
        binary_transfer = self.options['binary_transfer']
        request_complex_types = self.options['request_complex_types']

        self.write(messages.Startup(user, database, session_label, os_user_name, autocommit, binary_transfer, request_complex_types))

        while True:
            message = self.read_message()

            if isinstance(message, messages.Authentication):
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
                elif message.code == messages.Authentication.GSS:
                    self.make_GSS_authentication()
                else:
                    self.write(messages.Password(password, message.code,
                                                 {'user': user,
                                                  'salt': getattr(message, 'salt', None),
                                                  'usersalt': getattr(message, 'usersalt', None)}))
            elif isinstance(message, messages.BackendKeyData):
                self.backend_pid = message.pid
                self.backend_key = message.key
            elif isinstance(message, messages.ReadyForQuery):
                break
            elif isinstance(message, messages.ErrorResponse):
                self._logger.error(message.error_message())
                raise errors.ConnectionError(message.error_message())
            else:
                msg = "Received unexpected startup message: {0}".format(message)
                self._logger.error(msg)
                raise errors.MessageError(msg)
