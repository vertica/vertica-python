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

from abc import ABCMeta
from struct import pack

from ..messages import *


class Message(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @property
    def message_id(self):
        raise NotImplementedError("no default message_id")

    def _bytes_to_message(self, msg):

        if isinstance(msg, list):
            msg = ''.join(msg)

        if hasattr(msg, 'bytesize'):
            bytesize = msg.bytesize + 4
        else:
            bytesize = len(msg) + 4

        message_size = pack('!I', bytesize)
        if self.message_id is not None:
            msg_with_size = self.message_id + message_size + msg
        else:
            msg_with_size = message_size + msg

        return msg_with_size


# noinspection PyAbstractClass
class BackendMessage(Message):
    __metaclass__ = ABCMeta
    _message_id_map = {}

    @classmethod
    def from_type(cls, type_, data):
        klass = cls._message_id_map.get(type_)
        if klass is not None:
            return klass(data)
        else:
            from .backend_messages import Unknown
            return Unknown(type_, data)

    @staticmethod
    def register(cls):
        # TODO replace _message_id() with that
        assert issubclass(cls, BackendMessage), \
            "{0} is not subclass of BackendMessage".format(cls.__name__)
        assert cls.message_id not in BackendMessage._message_id_map, \
            "can't write the same key twice: {0}".format(cls.message_id)

        BackendMessage._message_id_map[cls.message_id] = cls


# noinspection PyAbstractClass
class FrontendMessage(Message):
    __metaclass__ = ABCMeta

    def fetch_message(self):
        """Generator for getting the message's content"""
        raise NotImplementedError("fetch_bytes has no default implementation")


# noinspection PyAbstractClass
class BulkFrontendMessage(FrontendMessage):
    __metaclass__ = ABCMeta

    def read_bytes(self):
        return b''

    def get_message(self):
        bytes_ = self.read_bytes()
        return self._bytes_to_message(bytes_)

    def fetch_message(self):
        yield self.get_message()


# noinspection PyAbstractClass
class StreamFrontendMessage(FrontendMessage):
    __metaclass__ = ABCMeta

    def stream_bytes(self):
        raise NotImplementedError("stream_bytes has no default implementation")

    def stream_message(self):
        for bytes_ in self.stream_bytes():
            yield self._bytes_to_message(bytes_)

    def fetch_message(self):
        for message in self.stream_message():
            yield message
