from __future__ import absolute_import

from vertica_python.vertica.messages.message import FrontendMessage

class Sync(FrontendMessage):
    pass


Sync._message_id('S')
