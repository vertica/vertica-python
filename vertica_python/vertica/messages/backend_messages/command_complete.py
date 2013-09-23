from __future__ import absolute_import

import re

from struct import unpack

from vertica_python.vertica.messages.message import BackendMessage

class CommandComplete(BackendMessage):

    def __init__(self, data):

        data = unpack('{0}sx'.format(len(data) - 1), data)[0]

        if re.match("INSERT", data) is not None:
            splitstr = data.split(' ', 3)
            self.tag = splitstr[0]
            if len(splitstr) >= 2:
                self.oid = int(splitstr[1])
            if len(splitstr) >= 3:
                self.rows = int(splitstr[2])
        elif re.match("(DELETE|UPDATE|MOVE|FETCH|COPY)", data) is not None:
            splitstr = data.split(' ', 2)
            self.tag = splitstr[0]
            if len(splitstr) >= 2:
                self.rows = int(splitstr[1])
        else:
            self.tag = data


CommandComplete._message_id('C')
