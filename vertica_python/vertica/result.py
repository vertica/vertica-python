from __future__ import absolute_import

from vertica_python.vertica.column import Column

class Result(object):

    def __init__(self, row_style='hash'):
        self.row_style = row_style
        self.rows = []

    def descriptions(self, message):
        self.columns = map(lambda fd: Column(fd), message.fields)

    def format_row_as_hash(self, row_data):
        row = {}
        for idx, value in enumerate(row_data.values):
            col = self.columns[idx]
            row[col.name] = col.convert(value)

        return row

    def format_row(self, row_data):
        return getattr(self, "format_row_as_{0}".format(self.row_style))(row_data)

    def format_row_as_array(self, row_data):
        row = []
        for idx, value in enumerate(row_data.values):
            row.append(self.columns[idx].convert(value))

        return row

    def add_row(self, row):
        self.rows.append(row)

    def is_empty(self):
        return self.row_count() == 0

    def the_value(self):
        if self.is_empty():
            return None

        if self.row_style == 'array':
            return self.rows[0][0]

        else:
            return self.rows[0][self.columns[0].name]

    def __getitem__(self, row, col=None):
        if col is None:
            return row[row]
        else:
            return self.rows[row][col]

    def row_count(self):
        return len(self.rows)

    def size(self):
        return self.row_count()

    def length(self):
        return self.row_count()
