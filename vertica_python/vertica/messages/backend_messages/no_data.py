from __future__ import absolute_import

from vertica_python.vertica.messages.message import BackendMessage

class NoData(BackendMessage):
    pass


NoData._message_id('n')
