from __future__ import print_function, division, absolute_import

from ..message import BackendMessage


class NoData(BackendMessage):
    message_id = b'n'


BackendMessage.register(NoData)
