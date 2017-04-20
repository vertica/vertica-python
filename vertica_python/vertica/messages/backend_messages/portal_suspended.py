from __future__ import print_function, division, absolute_import

from ..message import BackendMessage


class PortalSuspended(BackendMessage):
    message_id = b's'


BackendMessage.register(PortalSuspended)
