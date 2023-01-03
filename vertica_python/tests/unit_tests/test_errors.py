# Copyright (c) 2018-2023 Micro Focus or one of its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import print_function, division, absolute_import

from .base import VerticaPythonUnitTestCase
from ...errors import VerticaSyntaxError
from ...vertica.messages.backend_messages.error_response import ErrorResponse

import pickle

# Using a subclass of ErrorResponse for this test, to avoid the complexity of
# creating an ErrorResponse object. At the time of writing, ErrorResponse and
# other BackendMessage instances can only be created from server-provided data.
#
# This subclass allows for simpler instantiation without binding to any details
# of server data serialization, a la NoticeResponseAttrMixin and NoticeResponse
class MockErrorResponse(ErrorResponse):
    def __init__(self):
        # does NOT call super
        self._notice_attrs = {}

    def error_message(self):
        return "Manufactured error message for testing"

class ErrorsTestCase(VerticaPythonUnitTestCase):
    def test_pickling(self):
        err_response = MockErrorResponse()
        sql = "select 1;"
        exc = VerticaSyntaxError(err_response, sql)

        serde = pickle.loads(pickle.dumps(exc))

        assert isinstance(serde, VerticaSyntaxError)
        assert str(serde) == str(exc)
        assert isinstance(serde.error_response, MockErrorResponse)
        assert serde.error_response.error_message() == err_response.error_message()
        assert serde.sql == exc.sql
