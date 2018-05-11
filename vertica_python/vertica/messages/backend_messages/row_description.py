# Copyright (c) 2013-2017 Uber Technologies, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

from __future__ import print_function, division, absolute_import

from struct import unpack, unpack_from

from six.moves import range

from ..message import BackendMessage


class RowDescription(BackendMessage):
    message_id = b'T'

    def __init__(self, data):
        BackendMessage.__init__(self)
        self.fields = []
        field_count = unpack('!H', data[0:2])[0]
        pos = 2

        for i in range(field_count):
            field_info = unpack_from("!{0}sxIHIHIH".format(data.find(b'\x00', pos) - pos), data,
                                     pos)
            self.fields.append({
                'name': field_info[0],
                'table_oid': field_info[1],
                'attribute_number': field_info[2],
                'data_type_oid': field_info[3],
                'data_type_size': field_info[4],
                'type_modifier': field_info[5],
                'format_code': field_info[6],
            })

            pos += 19 + len(field_info[0])


BackendMessage.register(RowDescription)
