from __future__ import print_function, division, absolute_import

from ..message import FrontendMessage


class CopyDone(FrontendMessage):
    message_id = b'c'
