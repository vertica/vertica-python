from __future__ import absolute_import

from vertica_python.vertica.messages.message import BackendMessage

class CloseComplete(BackendMessage):
    pass


CloseComplete._message_id('3')
