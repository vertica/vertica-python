# Copyright (c) 2020-2023 Micro Focus or one of its affiliates.
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

from collections import namedtuple
from decimal import Decimal
from uuid import UUID
import datetime
import pytest

from ...vertica.cursor import Cursor
from .base import VerticaPythonUnitTestCase


class SqlLiteralTestCase(VerticaPythonUnitTestCase):

    def test_default_adapters(self):
        cursor = Cursor(None, self.logger)
        # None
        self.assertEqual(cursor.object_to_sql_literal(None), "NULL")
        # Boolean
        self.assertEqual(cursor.object_to_sql_literal(True), "True")
        self.assertEqual(cursor.object_to_sql_literal(False), "False")
        # Numeric
        self.assertEqual(cursor.object_to_sql_literal(123), "123")
        self.assertEqual(cursor.object_to_sql_literal(123.45), "123.45")
        self.assertEqual(cursor.object_to_sql_literal(Decimal("10.00000")), "10.00000")
        # UUID
        self.assertEqual(cursor.object_to_sql_literal(
            UUID('00010203-0405-0607-0809-0a0b0c0d0e0f')),
            "'00010203-0405-0607-0809-0a0b0c0d0e0f'")
        # Time
        self.assertEqual(cursor.object_to_sql_literal(
            datetime.datetime(2018, 9, 7, 15, 38, 19, 769000)), "'2018-09-07 15:38:19.769000'")
        self.assertEqual(cursor.object_to_sql_literal(datetime.date(2018, 9, 7)), "'2018-09-07'")
        self.assertEqual(cursor.object_to_sql_literal(datetime.time(13, 50, 9)), "'13:50:09'")
        # String
        self.assertEqual(cursor.object_to_sql_literal(u"string'1"), "'string''1'")
        self.assertEqual(cursor.object_to_sql_literal(b"string'1"), "'string''1'")
        # Tuple and namedtuple
        self.assertEqual(cursor.object_to_sql_literal(
            (123, u"string'1", None)), "(123,'string''1',NULL)")
        self.assertEqual(cursor.object_to_sql_literal(
            ((1, u"a"), (2, u"b"), (3, u"c"))), "((1,'a'),(2,'b'),(3,'c'))")
        Point = namedtuple('Point', ['x', 'y', 'z'])
        p = Point(x=11, y=22, z=33)
        self.assertEqual(cursor.object_to_sql_literal(p), "(11,22,33)")

    def test_register_adapters(self):
        class Point(object):
            def __init__(self, x, y):
                self.x = x
                self.y = y

        def adapt_point(point):
            return "STV_GeometryPoint({},{})".format(point.x, point.y)

        cursor = Cursor(None, self.logger)

        err_msg = "Please register a new adapter for this type"
        with pytest.raises(TypeError, match=err_msg):
            result = cursor.object_to_sql_literal(Point(-71.13, 42.36))

        err_msg = "The adapter is not callable"
        with pytest.raises(TypeError, match=err_msg):
            cursor.register_sql_literal_adapter(Point, "not-callable")

        cursor.register_sql_literal_adapter(Point, adapt_point)
        self.assertEqual(
            cursor.object_to_sql_literal(Point(-71.13, 42.36)), 
            "STV_GeometryPoint(-71.13,42.36)")
