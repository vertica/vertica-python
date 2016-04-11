

from struct import pack

from vertica_python.vertica.messages.message import FrontendMessage


class Parse(FrontendMessage):

    def __init__(self, name, query, param_types):
        self.name = name
        self.query = query
        self.param_types = param_types

    def to_bytes(self):
        params = ""
        for param in self.param_types:
            params = params + param

        return self.message_string(pack('!{0}sx{1}sxH{2}I'.format(len(self.name), len(self.query), len(self.param_types)), self.name, self.query, len(self.param_types), params))


Parse._message_id(b'P')
