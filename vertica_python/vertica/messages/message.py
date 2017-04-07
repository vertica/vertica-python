from __future__ import print_function, division, absolute_import

from struct import pack

from ..messages import *


class Message:
    def __init__(self):
        pass

    @property
    def message_id(self):
        raise NotImplementedError("no default message_id")

    def message_string(self, msg):

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
    _message_id_map = {}

    @classmethod
    def factory(cls, type_, data):
        klass = cls._message_id_map.get(type_)
        if klass is not None:
            return klass(data)
        else:
            return Unknown(type_, data)

    @staticmethod
    def register(cls):
        # TODO replace _message_id() with that
        assert issubclass(cls, BackendMessage), ("%s is not subclass of BackendMessage"
                                                 % (cls.__name__,))
        assert cls.message_id not in BackendMessage._message_id_map, \
            ("can't write the same key twice: '%s'" % (cls.message_id,))

        BackendMessage._message_id_map[cls.message_id] = cls


# noinspection PyAbstractClass
class FrontendMessage(Message):
    def to_bytes(self):
        return self.message_string(b'')
