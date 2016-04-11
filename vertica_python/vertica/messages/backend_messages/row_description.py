

from struct import unpack, unpack_from

from vertica_python.vertica.messages.message import BackendMessage


class RowDescription(BackendMessage):

    def __init__(self, data):

        self.fields = []
        field_count = unpack('!H', data[0:2])[0]
        pos = 2

        for i in range(field_count):
            field_info = unpack_from("!{0}sxIHIHIH".format(data.find(b'\x00', pos) - pos), data, pos)
            self.fields.append({
                'name': field_info[0],
                'table_oid': field_info[1],
                'attribute_number': field_info[2],
                'data_type_oid': field_info[3],
                'data_type_size': field_info[4],
                'type_modifier': field_info[5],
                'format_code': field_info[6],
            })

            pos += 19 + len(field_info[0])


RowDescription._message_id(b'T')
