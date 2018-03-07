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

from struct import pack

from ..message import BulkFrontendMessage


class Bind(BulkFrontendMessage):
    message_id = b'B'

    def __init__(self, portal_name, prepared_statement_name, parameter_values):
        BulkFrontendMessage.__init__(self)
        self._portal_name = portal_name
        self._prepared_statement_name = prepared_statement_name
        self._parameter_values = parameter_values

    def read_bytes(self):
        bytes_ = pack('!{0}sx{1}sxHH'.format(
            len(self._portal_name), len(self._prepared_statement_name)),
            self._portal_name, self._prepared_statement_name, 0, len(self._parameter_values))

        for val in self._parameter_values.values():
            if val is None:
                bytes_ += pack('!I', [-1])
            else:
                bytes_ += pack('!I{0}s'.format(len(val)), len(val), val)
        bytes_ += pack('!H', [0])

        return bytes_
