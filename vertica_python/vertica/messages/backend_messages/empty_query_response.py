from __future__ import absolute_import

from vertica_python.vertica.messages.message import BackendMessage

class EmptyQueryResponse(BackendMessage):
    pass


EmptyQueryResponse._message_id('I')
