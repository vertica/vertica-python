from __future__ import print_function, division, absolute_import

from struct import pack

from ..message import FrontendMessage


class Bind(FrontendMessage):
    message_id = b'B'

    def __init__(self, portal_name, prepared_statement_name, parameter_values):
        FrontendMessage.__init__(self)
        self.portal_name = portal_name
        self.prepared_statement_name = prepared_statement_name
        self.parameter_values = parameter_values

    def to_bytes(self):
        bytes_ = pack(
            '!{0}sx{1}sxHH'.format(len(self.portal_name), len(self.prepared_statement_name)),
            self.portal_name, self.prepared_statement_name, 0, len(self.parameter_values))
        for val in self.parameter_values.values():
            if val is None:
                bytes_ += pack('!I', [-1])
            else:
                bytes_ += pack('!I{0}s'.format(len(val)), len(val), val)
        bytes_ += pack('!H', [0])
        return self.message_string(bytes_)
