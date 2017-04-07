from __future__ import print_function, division, absolute_import

from struct import pack

from ..message import FrontendMessage


class Parse(FrontendMessage):
    message_id = b'P'

    def __init__(self, name, query, param_types):
        FrontendMessage.__init__(self)

        self.name = name
        self.query = query
        self.param_types = param_types

    def to_bytes(self):
        params = ""
        for param in self.param_types:
            params = params + param

        return self.message_string(
            pack('!{0}sx{1}sxH{2}I'.format(len(self.name), len(self.query), len(self.param_types)),
                 self.name, self.query, len(self.param_types), params))
