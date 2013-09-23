from __future__ import absolute_import

import string

from struct import unpack_from

from vertica_python.vertica.messages.message import BackendMessage

class NoticeResponse(BackendMessage):

    FIELDS_DEFINITIONS = [
        {'type': 'q', 'name': "Internal Query", 'method': 'internal_query'},
        {'type': 'S', 'name': "Severity", 'method': 'severity'},
        {'type': 'M', 'name': "Message", 'method': 'message'},
        {'type': 'C', 'name': "Sqlstate", 'method': 'sqlstate'},
        {'type': 'D', 'name': "Detail", 'method': 'detail'},
        {'type': 'H', 'name': "Hint", 'method': 'hint'},
        {'type': 'P', 'name': "Position", 'method': 'position'},
        {'type': 'W', 'name': "Where", 'method': 'where'},
        {'type': 'p', 'name': "Internal Position", 'method': 'internal_position'},
        {'type': 'R', 'name': "Routine", 'method': 'routine'},
        {'type': 'F', 'name': "File", 'method': 'file'},
        {'type': 'L', 'name': "Line", 'method': 'line'}
    ]

    def FIELDS(self):
        pairs = []
        for field in self.FIELDS_DEFINITIONS:
            pairs.append((field['type'], field['name']))
        return dict(pairs)

    def __init__(self, data):
        self.values = {}

        pos = 0
        while pos < len(data) - 1:
            null_byte = string.find(data, '\x00', pos)
            
            # This will probably work
            unpacked = unpack_from('c{0}sx'.format(null_byte - 1 - pos), data, pos)
            key = unpacked[0]
            value = unpacked[1]

            self.values[self.FIELDS()[key]] = value
            pos += (len(value) + 2)

        # May want to break out into a function at some point
        for field_def in self.FIELDS_DEFINITIONS:
            if self.values.get(field_def['name'], None) is not None:
                setattr(self, field_def['method'], self.values[field_def['name']])

    def error_message(self):
        ordered = []
        for field in self.FIELDS_DEFINITIONS:
            if self.values.get(field['name']) is not None:
                ordered.append("{0}: {1}".format(field['name'], self.values[field['name']]))
        return ', '.join(ordered)


NoticeResponse._message_id('N')
