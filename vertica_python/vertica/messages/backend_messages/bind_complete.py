from __future__ import print_function, division, absolute_import

from ..message import BackendMessage


class BindComplete(BackendMessage):
    message_id = b'2'


BackendMessage.register(BindComplete)
