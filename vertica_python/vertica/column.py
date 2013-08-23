from __future__ import absolute_import

from decimal import Decimal
from datetime import date
from dateutil import parser

class Column(object):

    DATA_TYPE_CONVERSIONS = [
        ['unspecified', None],
        ['tuple', None],
        ['pos', None],
        ['record', None],
        ['unknown', None],
        ['bool', lambda s: s == 't'],
        ['integer', lambda s: int(s)],
        ['float', lambda s: float(s)],
        ['char', lambda s: unicode(s, 'utf-8')],
        ['varchar', lambda s: unicode(s, 'utf-8')],
        ['date', lambda s: date(*map(lambda x: int(x), s.split('-')))],
        ['time', None],
        ['timestamp', lambda s: parser.parse(s)],
        ['timestamp_tz', lambda s: parser.parse(s)],
        ['interval', None],
        ['time_tz', None],
        ['numeric', lambda s: Decimal(s)],
        ['bytea', None],
        ['rle_tuple', None],
    ]

    DATA_TYPES = map(lambda x: x[0], DATA_TYPE_CONVERSIONS)

    def __init__(self, col):
        self.type_modifier = col['type_modifier']
        self.format = 'text' if col['format_code'] == 0 else 'binary'
        self.table_oid = col['table_oid']
        self.name = col['name']
        self.attribute_number = col['attribute_number']
        self.data_type = self.DATA_TYPE_CONVERSIONS[col['data_type_oid']][0]
        self.converter = self.DATA_TYPE_CONVERSIONS[col['data_type_oid']][1]
        self.size = col['data_type_size']

    def convert(self, s):
        if s is None:
            return
        return self.converter(s) if self.converter is not None else s
