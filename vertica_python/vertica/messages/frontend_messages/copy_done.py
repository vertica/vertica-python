from __future__ import absolute_import

from vertica_python.vertica.messages.message import FrontendMessage

class CopyDone(FrontendMessage):
    pass


CopyDone._message_id('c')
