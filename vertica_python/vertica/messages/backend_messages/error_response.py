from __future__ import print_function, division, absolute_import

from ..message import BackendMessage
from vertica_python.vertica.messages.backend_messages.notice_response import NoticeResponse


class ErrorResponse(NoticeResponse, BackendMessage):
    message_id = b'E'


BackendMessage.register(ErrorResponse)
