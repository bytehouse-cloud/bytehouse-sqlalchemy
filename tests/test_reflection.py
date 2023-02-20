"""
This is the MIT license: http://www.opensource.org/licenses/mit-license.php

Copyright (c) 2017 by Konstantin Lebedev.

Copyright 2022- 2023 Bytedance Ltd. and/or its affiliates

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import enum
import unittest

from sqlalchemy import Column, inspect, func, types as sa_types

from bytehouse_sqlalchemy import types, engines, Table
from tests.testcase import BaseTestCase
from tests.util import require_server_version, with_native_sessions


@with_native_sessions
class ReflectionTestCase(BaseTestCase):

    def test_server_default(self):
        metadata = self.metadata()

        args = (
            [Column('x1', sa_types.String),
             Column('x2', sa_types.String, server_default='')] +
            [engines.CnchMergeTree(
                order_by=func.tuple()
            )]
        )

        table = Table('t', metadata, *args)
        with self.create_table(table):
            self.assertEqual([
                c['default'] for c in inspect(metadata.bind
                                              ).get_columns('t')
            ], [None, "''"])

    def _type_round_trip(self, *types):
        metadata = self.metadata()
        args = (
            [Column('t%d' % i, type_) for i, type_ in enumerate(types)] +
            [engines.CnchMergeTree(
                order_by=func.tuple()
            )]
        )

        table = Table('t', metadata, *args)
        with self.create_table(table):
            return inspect(metadata.bind).get_columns('t')

    def test_array(self):
        coltype = self._type_round_trip(types.Array(types.Int32))[0]['type']

        self.assertIsInstance(coltype, types.Array)
        self.assertEqual(coltype.item_type, types.Int32)

    def test_array_of_array(self):
        coltype = self._type_round_trip(
            types.Array(types.Array(types.Int32))
        )[0]['type']

        self.assertIsInstance(coltype, types.Array)
        self.assertIsInstance(coltype.item_type, types.Array)
        self.assertEqual(coltype.item_type.item_type, types.Int32)

    def test_sting_length(self):
        coltype = self._type_round_trip(types.String(10))[0]['type']

        self.assertIsInstance(coltype, types.String)
        self.assertEqual(coltype.length, 10)

    def test_nullable(self):
        col = self._type_round_trip(types.Nullable(types.Int32))[0]

        self.assertIsInstance(col['type'], types.Nullable)
        self.assertTrue(col['nullable'])
        self.assertEqual(col['type'].nested_type, types.Int32)

    def test_not_null(self):
        metadata = self.metadata()
        table = Table(
            't', metadata,
            Column('x', types.Int32, nullable=False),
            engines.CnchMergeTree(
                order_by=func.tuple()
            )
        )
        with self.create_table(table):
            col = inspect(metadata.bind).get_columns('t')[0]

        self.assertIsInstance(col['type'], types.Int32)
        self.assertFalse(col['nullable'])

    @require_server_version(19, 3, 3)
    def test_low_cardinality(self):
        coltype = self._type_round_trip(
            types.LowCardinality(types.String)
        )[0]['type']

        self.assertIsInstance(coltype, types.LowCardinality)
        self.assertEqual(coltype.nested_type, types.String)

    def test_tuple(self):
        coltype = self._type_round_trip(
            types.Tuple(types.String, types.Int32)
        )[0]['type']

        self.assertIsInstance(coltype, types.Tuple)
        self.assertEqual(coltype.nested_types[0], types.String)
        self.assertEqual(coltype.nested_types[1], types.Int32)

    @require_server_version(21, 1, 3)
    def test_map(self):
        coltype = self._type_round_trip(
            types.Map(types.String, types.String)
        )[0]['type']

        self.assertIsInstance(coltype, types.Map)
        self.assertEqual(coltype.key_type, types.String)
        self.assertEqual(coltype.value_type, types.String)

    def test_enum8(self):
        enum_options = {'three': 3, "quoted' ": 9, 'comma, ': 14}
        coltype = self._type_round_trip(
            types.Enum8(enum.Enum('any8_enum', enum_options))
        )[0]['type']

        self.assertIsInstance(coltype, types.Enum8)
        self.assertEqual(
            {o.name: o.value for o in coltype.enum_class}, enum_options
        )

    def test_enum16(self):
        enum_options = {'first': 1024, 'second': 2048}
        coltype = self._type_round_trip(
            types.Enum16(enum.Enum('any16_enum', enum_options))
        )[0]['type']

        self.assertIsInstance(coltype, types.Enum16)
        self.assertEqual(
            {o.name: o.value for o in coltype.enum_class}, enum_options
        )

    def test_decimal(self):
        coltype = self._type_round_trip(types.Decimal(38, 38))[0]['type']

        self.assertIsInstance(coltype, types.Decimal)
        self.assertEqual(coltype.precision, 38)
        self.assertEqual(coltype.scale, 38)
