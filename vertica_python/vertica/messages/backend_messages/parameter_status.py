from __future__ import absolute_import

import string

from struct import unpack

from vertica_python.vertica.messages.message import BackendMessage

class ParameterStatus(BackendMessage):

    def __init__(self, data):
        null_byte = string.find(data, '\x00')
        unpacked = unpack('{0}sx{1}sx'.format(null_byte - 1, len(data) - null_byte - 1), data)
        self.name = unpacked[0]
        self.value = unpacked[1]


ParameterStatus._message_id('S')
