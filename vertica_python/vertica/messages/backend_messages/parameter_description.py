from __future__ import print_function, division, absolute_import

from struct import unpack, unpack_from

from ..message import BackendMessage
from vertica_python.vertica.column import Column


class ParameterDescription(BackendMessage):
    message_id = b't'

    def __init__(self, data):
        BackendMessage.__init__(self)
        parameter_count = unpack('!H', data)[0]
        parameter_type_ids = unpack_from("!{0}N".format(parameter_count), data, 2)
        self.parameter_types = [Column.data_types()[dtid] for dtid in parameter_type_ids]


BackendMessage.register(ParameterDescription)
