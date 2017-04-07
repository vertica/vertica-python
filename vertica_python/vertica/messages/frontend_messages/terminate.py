from __future__ import print_function, division, absolute_import

from ..message import FrontendMessage


class Terminate(FrontendMessage):
    message_id = b'X'
