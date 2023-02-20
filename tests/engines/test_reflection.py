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

from contextlib import contextmanager
from unittest.mock import Mock

from sqlalchemy import Column, text, create_engine, inspect

from bytehouse_sqlalchemy import types, engines, Table, make_session
from tests.testcase import BaseTestCase
from tests.util import with_native_sessions


@with_native_sessions
class EngineReflectionTestCase(BaseTestCase):
    required_server_version = (18, 16, 0)

    @contextmanager
    def _test_table(self, engine, *columns):
        metadata = self.metadata()
        columns = list(columns) + [Column('x', types.UInt32)] + [engine]
        table = Table('test_reflect', metadata, *columns)

        with self.create_table(table):
            metadata.clear()  # reflect from clean state
            self.assertFalse(metadata.tables)
            table = Table('test_reflect', metadata, autoload=True)
            yield table, table.engine

    def assertColumns(self, first, second, msg=None):
        self.assertListEqual(list(first), second, msg=msg)

    def test_merge_tree(self):
        engine = engines.CnchMergeTree(
            partition_by='x', order_by='x', primary_key='x', sample_by='x'
        )

        with self._test_table(engine) as (table, engine):
            self.assertIsInstance(engine, engines.CnchMergeTree)
            self.assertColumns(engine.partition_by.columns, [table.c.x])
            self.assertColumns(engine.order_by.columns, [table.c.x])
            self.assertColumns(engine.primary_key.columns, [table.c.x])
            self.assertColumns(engine.sample_by.columns, [table.c.x])

    def test_merge_tree_param_expressions(self):
        engine = engines.CnchMergeTree(
            partition_by=text('toYYYYMM(toDate(x))'),
            order_by='x', primary_key='x'
        )

        with self._test_table(engine) as (table, engine):
            self.assertIsInstance(engine, engines.CnchMergeTree)
            self.assertEqual(
                str(engine.partition_by.expressions[0]), 'toYYYYMM(toDate(x))'
            )
            self.assertColumns(engine.order_by.columns, [table.c.x])
            self.assertColumns(engine.primary_key.columns, [table.c.x])

    def test_create_reflected(self):
        metadata = self.metadata()

        table = Table(
            'test_reflect', metadata,
            Column('x', types.Int32),
            engines.CnchMergeTree(partition_by='x', order_by='x')
        )

        with self.create_table(table):
            metadata.clear()  # reflect from clean state
            self.assertFalse(metadata.tables)
            table = Table('test_reflect', metadata, autoload=True)

            exists_query = 'EXISTS TABLE test_reflect'
            table.drop()
            exists = self.session.execute(exists_query).fetchall()
            self.assertEqual(exists, [(0, )])

            table.create()
            exists = self.session.execute(exists_query).fetchall()
            self.assertEqual(exists, [(1, )])
