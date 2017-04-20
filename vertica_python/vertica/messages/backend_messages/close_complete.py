from __future__ import print_function, division, absolute_import

from ..message import BackendMessage


class CloseComplete(BackendMessage):
    message_id = b'3'


BackendMessage.register(CloseComplete)
