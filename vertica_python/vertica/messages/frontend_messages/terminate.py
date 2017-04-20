from __future__ import print_function, division, absolute_import

from ..message import BulkFrontendMessage


class Terminate(BulkFrontendMessage):
    message_id = b'X'
