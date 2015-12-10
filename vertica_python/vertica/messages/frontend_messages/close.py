

from struct import pack

from vertica_python.vertica.messages.message import FrontendMessage


class Close(FrontendMessage):

    def __init__(self, close_type, close_name):
        self.close_name = close_name

        if close_type == 'portal':
            self.close_type = 'P'
        elif close_type == 'prepared_statement':
            self.close_type = 'S'
        else:
            raise ValueError("{0} is not a valid close_type.  Must be either portal or prepared_statement".format(close_type))

    def to_bytes(self):
        return self.message_string(pack('c{0}sx'.format(len(self.close_name)), self.close_type, self.close_name))


Close._message_id(b'C')
