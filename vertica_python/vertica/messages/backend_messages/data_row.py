from __future__ import absolute_import

from struct import unpack, unpack_from

from vertica_python.vertica.messages.message import BackendMessage

class DataRow(BackendMessage):

    def __init__(self, data):
        self.values = []
        field_count = unpack('!H', data[0:2])[0]
        pos = 2

        for i in xrange(field_count):
            size = unpack_from('!I', data, pos)[0]

            if size == 4294967295:
                size = -1

            if size == -1:
                self.values.append(None)
            else:
                self.values.append(unpack_from('{0}s'.format(size), data, pos + 4)[0])
            pos += (4 + max(size, 0))


DataRow._message_id('D')
