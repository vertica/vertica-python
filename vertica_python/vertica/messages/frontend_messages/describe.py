

from struct import pack

from vertica_python.vertica.messages.message import FrontendMessage


class Describe(FrontendMessage):

    def __init__(self, describe_type, describe_name):
        self.describe_name = describe_name

        if describe_type == 'portal':
            self.describe_type = 'P'
        elif describe_type == 'prepared_statement':
            self.describe_type = 'S'
        else:
            raise ValueError("{0} is not a valid describe_type.  Must be either portal or prepared_statement".format(describe_type))

    def to_bytes(self):
        return self.message_string(pack('c{0}sx'.format(len(self.describe_name)), self.describe_type, self.describe_name))


Describe._message_id(b'D')
