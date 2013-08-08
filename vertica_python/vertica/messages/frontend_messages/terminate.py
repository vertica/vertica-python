from __future__ import absolute_import

from vertica_python.vertica.messages.message import FrontendMessage

class Terminate(FrontendMessage):
    pass


Terminate._message_id('X')
