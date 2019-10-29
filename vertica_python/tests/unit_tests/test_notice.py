# Copyright (c) 2018-2019 Micro Focus or one of its affiliates.
# Copyright (c) 2018 Uber Technologies, Inc.
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

# Copyright (c) 2013-2017 Uber Technologies, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

from __future__ import print_function, division, absolute_import

import mock

from .base import VerticaPythonUnitTestCase
from ...vertica.messages import NoticeResponse
from ...errors import QueryError

class NoticeTestCase(VerticaPythonUnitTestCase):
    SAMPLE_DATA = {b'S': 'FATAL',
                   b'H': 'This is a test hint',
                   b'L': '9999',
                   b'M': 'Failure is on purpose'}

    @mock.patch.object(NoticeResponse, '_unpack_data')
    def test_error_message(self, mock_unpack_data):
        mock_unpack_data.return_value = NoticeTestCase.SAMPLE_DATA

        notice = NoticeResponse(b'ignored-due-to-mock')
        self.assertEqual(
            notice.error_message(),
            'Severity: FATAL, Message: Failure is on purpose, Hint: This is a test hint, Line: 9999'
        )

    @mock.patch.object(NoticeResponse, '_unpack_data')
    def test_attribute_properties(self, mock_unpack_data):
        mock_unpack_data.return_value = NoticeTestCase.SAMPLE_DATA

        notice = NoticeResponse(b'ignored-due-to-mock')
        self.assertEqual(notice.severity, 'FATAL')
        self.assertEqual(notice.hint, 'This is a test hint')
        # yes, line is still a string.
        self.assertEqual(notice.line, '9999')
        self.assertEqual(notice.message, 'Failure is on purpose')
        self.assertIsNone(notice.detail)
        self.assertIsNone(notice.sqlstate)

    @mock.patch.object(NoticeResponse, '_unpack_data')
    def test_labeled_values(self, mock_unpack_data):
        mock_unpack_data.return_value = NoticeTestCase.SAMPLE_DATA

        notice = NoticeResponse(b'ignored-due-to-mock')
        self.assertEqual(notice.values, {
            'Severity': 'FATAL',
            'Hint': 'This is a test hint',
            'Line': '9999',
            'Message': 'Failure is on purpose'})

    @mock.patch.object(NoticeResponse, '_unpack_data')
    def test_query_error(self, mock_unpack_data):
        mock_unpack_data.return_value = NoticeTestCase.SAMPLE_DATA

        notice = NoticeResponse(b'ignored-due-to-mock')
        query_error = QueryError(notice, 'Select Fake();')

        self.assertEqual(query_error.severity, 'FATAL')
        self.assertEqual(query_error.hint, 'This is a test hint')
        self.assertEqual(query_error.line, '9999')
        self.assertEqual(query_error.message, 'Failure is on purpose')
        self.assertIsNone(query_error.detail)
        self.assertIsNone(query_error.sqlstate)

        self.assertEqual(
            str(query_error),
            'Severity: FATAL, Message: Failure is on purpose, Hint: This is a test hint, Line: 9999, SQL: \'Select Fake();\'')
