from __future__ import absolute_import

from vertica_python.vertica.messages.message import BackendMessage

class PortalSuspended(BackendMessage):
    pass


PortalSuspended._message_id('s')
