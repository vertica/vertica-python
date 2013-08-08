from __future__ import absolute_import

from vertica_python.vertica.messages.message import BackendMessage

class BindComplete(BackendMessage):
    pass


BindComplete._message_id('2')
