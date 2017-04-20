from __future__ import print_function, division, absolute_import

from ..message import BackendMessage


class EmptyQueryResponse(BackendMessage):
    message_id = b'I'

    def __init__(self, data=None):
        BackendMessage.__init__(self)
        self.data = data


BackendMessage.register(EmptyQueryResponse)
