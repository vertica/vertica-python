from __future__ import print_function, division, absolute_import

from ..message import BulkFrontendMessage


class Flush(BulkFrontendMessage):
    message_id = b'H'
