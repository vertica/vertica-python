from __future__ import print_function, division, absolute_import

from ..message import FrontendMessage


class Flush(FrontendMessage):
    message_id = b'H'
