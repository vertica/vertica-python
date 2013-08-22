from __future__ import absolute_import

from vertica_python.vertica.messages.message import FrontendMessage

class Flush(FrontendMessage):
    pass


Flush._message_id('H')
