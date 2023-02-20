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

from sqlalchemy import Column, text, Table, inspect, func
from bytehouse_sqlalchemy import types, Table as CHTable, engines
from tests.testcase import BaseTestCase
from tests.util import with_native_sessions


@with_native_sessions
class SchemaTestCase(BaseTestCase):

    def test_reflect(self):
        # checking, that after call metadata.reflect()
        # we have a clickhouse-specific table, which has overridden join
        # methods

        session = self.session
        metadata = self.metadata()

        # ### Maybe: ###
        # # session = native_session  # database=test
        # session = self.__class__.session
        # same as in `self.metadata()`  # database=default
        # unbound_metadata = MetaData(bind=session.bind)

        table = CHTable(
            'test_reflect',
            metadata,
            Column('x', types.Int32),
            engines.CnchMergeTree(
                order_by=func.tuple()
            )
        )

        table.drop(session.bind, if_exists=True)
        table.create(session.bind)

        # Sub-test: ensure the `metadata.reflect` makes a CHTable
        metadata.clear()  # reflect from clean state
        self.assertFalse(metadata.tables)
        metadata.reflect(only=[table.name])
        table2 = metadata.tables.get(table.name)
        self.assertIsNotNone(table2)
        self.assertListEqual([c.name for c in table2.columns], ['x'])
        self.assertTrue(isinstance(table2, CHTable))

        # Sub-test: ensure `CHTable(..., autoload=True)` works too
        metadata.clear()
        table3 = CHTable('test_reflect', metadata, autoload=True)
        self.assertListEqual([c.name for c in table3.columns], ['x'])

        # Sub-test: check that they all reflected the same.
        for table_x in [table, table2, table3]:
            query = table_x.select().select_from(table_x.join(
                text('another_table'),
                table.c.x == 'xxx',
                type='INNER',
                strictness='ALL',
                distribution='GLOBAL'
            ))
            self.assertEqual(
                self.compile(query),
                "SELECT test_reflect.x FROM test_reflect "
                "GLOBAL ALL INNER JOIN another_table "
                "ON test_reflect.x = %(x_1)s"
            )

    def test_reflect_generic_table(self):
        # checking, that generic table columns are reflected properly

        metadata = self.metadata()

        table = Table(
            'test_reflect',
            metadata,
            Column('x', types.Int32),
            engines.CnchMergeTree(
                order_by=func.tuple()
            )
        )

        self.session.execute('DROP TABLE IF EXISTS test_reflect')
        table.create(self.session.bind)

        # Sub-test: ensure the `metadata.reflect` makes a CHTable
        metadata.clear()  # reflect from clean state
        self.assertFalse(metadata.tables)

        table = Table('test_reflect', metadata, autoload=True)
        self.assertListEqual([c.name for c in table.columns], ['x'])

    def test_get_schema_names(self):
        insp = inspect(self.session.bind)
        schema_names = insp.get_schema_names()
        self.assertGreater(len(schema_names), 0)

    def test_get_table_names(self):
        table = Table(
            'test_reflect',
            self.metadata(),
            Column('x', types.Int32),
            engines.CnchMergeTree(
                order_by=func.tuple()
            )
        )

        self.session.execute('DROP TABLE IF EXISTS test_reflect')
        table.create(self.session.bind)

        insp = inspect(self.session.bind)
        # TODO: Investigate get_table_names should return all table names / w.r.t current session
        self.assertIn('test_reflect', insp.get_table_names())
