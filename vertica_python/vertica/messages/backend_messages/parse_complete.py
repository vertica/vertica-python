from __future__ import absolute_import

from vertica_python.vertica.messages.message import BackendMessage

class ParseComplete(BackendMessage):
    pass


ParseComplete._message_id('1')
