

from struct import pack

from vertica_python.vertica.messages.message import FrontendMessage


class Execute(FrontendMessage):

    def __init__(self, portal_name, max_rows):
        self.portal_name = portal_name
        self.max_rows = max_rows

    def to_bytes(self):
        return self.message_string(pack('!{0}sxI'.format(len(self.portal_name)), self.portal_name, self.max_rows))


Execute._message_id(b'E')
