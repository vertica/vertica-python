

from struct import pack

from vertica_python.vertica.messages.message import FrontendMessage


class CopyFail(FrontendMessage):

    def __init__(self, error_message):
        self.error_message = error_message

    def to_bytes(self):
        return self.message_string(pack('{0}sx'.format(len(self.error_message)), self.error_message))


CopyFail._message_id(b'f')
