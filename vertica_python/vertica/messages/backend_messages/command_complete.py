from __future__ import print_function, division, absolute_import

import re

from struct import unpack

from ..message import BackendMessage


class CommandComplete(BackendMessage):
    message_id = b'C'

    def __init__(self, data):
        BackendMessage.__init__(self)
        data = unpack('{0}sx'.format(len(data) - 1), data)[0]

        if re.match(b"INSERT", data) is not None:
            splitstr = data.split(b' ', 3)
            self.tag = splitstr[0]
            if len(splitstr) >= 2:
                self.oid = int(splitstr[1])
            if len(splitstr) >= 3:
                self.rows = int(splitstr[2])
        elif re.match(b"(DELETE|UPDATE|MOVE|FETCH|COPY)", data) is not None:
            splitstr = data.split(b' ', 2)
            self.tag = splitstr[0]
            if len(splitstr) >= 2:
                self.rows = int(splitstr[1])
        else:
            self.tag = data


BackendMessage.register(CommandComplete)
