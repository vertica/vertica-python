

from vertica_python.vertica.messages.message import BackendMessage
from vertica_python.vertica.messages.backend_messages.notice_response import NoticeResponse


class ErrorResponse(NoticeResponse, BackendMessage):
    pass


ErrorResponse._message_id(b'E')
