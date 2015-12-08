

from vertica_python.vertica.messages.message import BackendMessage


class PortalSuspended(BackendMessage):
    pass


PortalSuspended._message_id(b's')
