from __future__ import print_function, division, absolute_import

from ..message import BackendMessage


class ParseComplete(BackendMessage):
    message_id = b'1'


BackendMessage.register(ParseComplete)
