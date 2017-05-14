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
