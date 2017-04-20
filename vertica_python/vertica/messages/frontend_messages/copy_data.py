from __future__ import print_function, division, absolute_import

from six import text_type, binary_type

from ..message import BulkFrontendMessage

UTF_8 = 'utf-8'


class CopyData(BulkFrontendMessage):
    message_id = b'd'

    def __init__(self, data, unicode_error='strict'):
        BulkFrontendMessage.__init__(self)
        self._unicode_error = unicode_error
        if isinstance(data, text_type):
            self._data = self._data.encode(encoding=UTF_8, errors=self._unicode_error)
        elif isinstance(data, binary_type):
            self._data = data
        else:
            raise TypeError("should be string or bytes")

    def read_bytes(self):
        # to deal with unicode strings
        bytes_ = self._data
        return bytes_
