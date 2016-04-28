

import types

from struct import pack

from vertica_python.vertica.messages import *


class Message(object):

    @classmethod
    def _message_id(cls, message_id):
        instance_message_id = message_id

        def message_id(self):
            return instance_message_id
        setattr(cls, 'message_id', types.MethodType(message_id, cls))

    def message_string(self, msg):

        if isinstance(msg, list):
            msg = ''.join(msg)

        if hasattr(msg, 'bytesize'):
            bytesize = msg.bytesize + 4
        else:
            bytesize = len(msg) + 4

        message_size = pack('!I', bytesize)
        if self.message_id() is not None:
            msg_with_size = self.message_id() + message_size + msg
        else:
            msg_with_size = message_size + msg

        return msg_with_size


class BackendMessage(Message):
    MessageIdMap = {}

    @classmethod
    def factory(cls, type_, data):
        klass = cls.MessageIdMap[type_]
        if klass is not None:
            return klass(data)
        else:
            return messages.Unknown(type_, data)

    @classmethod
    def _message_id(cls, message_id):
        super(BackendMessage, cls)
        cls.MessageIdMap[message_id] = cls


class FrontendMessage(Message):
    def to_bytes(self):
        return self.message_string(b'')
