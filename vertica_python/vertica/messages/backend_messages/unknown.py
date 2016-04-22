

from vertica_python.vertica.messages.message import BackendMessage


class Unknown(BackendMessage):

    def __init__(self, message_id, data):
        self.message_id = message_id
        self.data = data
