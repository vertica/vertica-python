

from struct import unpack, unpack_from

from vertica_python.vertica.messages.message import BackendMessage
from vertica_python.vertica.column import Column


class ParameterDescription(BackendMessage):

    def __init__(self, data):
        parameter_count = unpack('!H', data)[0]
        parameter_type_ids = unpack_from("!{0}N".format(parameter_count), data, 2)
        self.parameter_types = [Column.data_types[dtid] for dtid in parameter_type_ids]


ParameterDescription._message_id(b't')
