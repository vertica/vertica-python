from __future__ import print_function, division, absolute_import

from ..message import BulkFrontendMessage


class Sync(BulkFrontendMessage):
    message_id = b'S'
