# Copyright (c) 2013-2017 Uber Technologies, Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
