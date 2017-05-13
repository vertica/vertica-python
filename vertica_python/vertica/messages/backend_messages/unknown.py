from __future__ import print_function, division, absolute_import

from ..message import BackendMessage


class Unknown(BackendMessage):
    def __init__(self, message_id, data):
        BackendMessage.__init__(self)
        self._message_id = message_id
        self.data = data

    @property
    def message_id(self):
        return self._message_id
