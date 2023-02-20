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

import unittest

from sqlalchemy import Column, func, text, select, inspect
from sqlalchemy.sql.ddl import CreateTable, CreateColumn

from bytehouse_sqlalchemy import (
    types, engines, Table, get_declarative_base, MaterializedView
)
from bytehouse_sqlalchemy.sql.ddl import DropTable
from tests.testcase import BaseTestCase
from tests.session import mocked_engine
from tests.util import require_server_version


class DDLTestCase(BaseTestCase):
    def test_create_table(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.String),
            Column('z', types.String(10)),
            # Must be quoted:
            Column('index', types.String),
            engines.CnchMergeTree(
                order_by=func.tuple()
            )
        )

        # No NOT NULL. And any PKS.
        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 ('
            'x Int32, y String, z FixedString(10), "index" String) '
            'ENGINE = CnchMergeTree() ORDER BY tuple()'
        )

    def test_create_table_nested_types(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Array(types.String)),
            engines.CnchMergeTree(
                order_by=func.tuple()
            )
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 '
            '(x Int32, y Array(String)) '
            'ENGINE = CnchMergeTree() ORDER BY tuple()'
        )

        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Array(types.Array(types.String))),
            engines.CnchMergeTree(
                order_by=func.tuple()
            )
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 '
            '(x Int32, y Array(Array(String))) '
            'ENGINE = CnchMergeTree() ORDER BY tuple()'
        )

        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Array(types.Array(types.String))),
            engines.CnchMergeTree(
                order_by=func.tuple()
            )
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 '
            '(x Int32, y Array(Array(String))) '
            'ENGINE = CnchMergeTree() ORDER BY tuple()'
        )

    def test_create_table_nullable(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Nullable(types.String)),
            Column('z', types.Nullable(types.String(10))),
            engines.CnchMergeTree(
                order_by=func.tuple()
            )
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 '
            '(x Int32, y Nullable(String), z Nullable(FixedString(10))) '
            'ENGINE = CnchMergeTree() ORDER BY tuple()'
        )

    def test_create_table_nested(self):
        table = Table(
            't1',
            self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('parent', types.Nested(
                Column('child1', types.Int32),
                Column('child2', types.String),
            )),
            engines.CnchMergeTree(
                order_by=func.tuple()
            )
        )
        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 ('
            'x Int32, '
            'parent Nested('
            'child1 Int32, '
            "child2 String"
            ')'
            ') ENGINE = CnchMergeTree() ORDER BY tuple()'
        )

    def test_create_table_nested_nullable(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Array(types.Nullable(types.String))),
            engines.CnchMergeTree(
                order_by=func.tuple()
            )
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 '
            '(x Int32, y Array(Nullable(String))) '
            'ENGINE = CnchMergeTree() ORDER BY tuple()'
        )

    def test_create_table_nullable_nested_nullable(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Nullable(
                types.Array(types.Nullable(types.String)))
            ),
            engines.CnchMergeTree(
                order_by=func.tuple()
            )
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 '
            '(x Int32, y Nullable(Array(Nullable(String)))) '
            'ENGINE = CnchMergeTree() ORDER BY tuple()'
        )

    def test_create_table_with_codec(self):
        table = Table(
            't1', self.metadata(),
            Column(
                'list',
                types.DateTime,
                bytehouse_codec=['DoubleDelta', 'ZSTD'],
            ),
            Column(
                'tuple',
                types.UInt8,
                bytehouse_codec=('T64', 'ZSTD(5)'),
            ),
            Column('explicit_none', types.UInt32, bytehouse_codec=None),
            Column('str', types.Int8, bytehouse_codec='ZSTD'),
            engines.CnchMergeTree(
                order_by=func.tuple()
            )
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 ('
            'list DateTime CODEC(DoubleDelta, ZSTD), '
            'tuple UInt8 CODEC(T64, ZSTD(5)), '
            'explicit_none UInt32, '
            'str Int8 CODEC(ZSTD)) '
            'ENGINE = CnchMergeTree() ORDER BY tuple()'
        )

    def test_create_table_column_default(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int8),
            Column('dt', types.DateTime, server_default=func.now()),
            engines.CnchMergeTree(
                order_by=func.tuple()
            )
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 ('
            'x Int8, '
            'dt DateTime DEFAULT now()) '
            'ENGINE = CnchMergeTree() ORDER BY tuple()'
        )

    def test_create_table_column_default_another_column(self):
        class TestTable(get_declarative_base()):
            x = Column(types.Int8, primary_key=True)
            y = Column(types.Int8, server_default=x)

            __table_args__ = (
                engines.CnchMergeTree(
                    order_by=func.tuple()
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table ('
            'x Int8, '
            'y Int8 DEFAULT x) '
            'ENGINE = CnchMergeTree() ORDER BY tuple()'
        )

    def test_create_table_column_materialized(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int8),
            Column('dt', types.DateTime, bytehouse_materialized=func.now()),
            engines.CnchMergeTree(
                order_by=func.tuple()
            )
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 ('
            'x Int8, '
            'dt DateTime MATERIALIZED now()) '
            'ENGINE = CnchMergeTree() ORDER BY tuple()'
        )

    def test_create_table_column_materialized_another_column(self):
        class TestTable(get_declarative_base()):
            x = Column(types.Int8, primary_key=True)
            y = Column(types.Int8, bytehouse_materialized=x)

            __table_args__ = (
                engines.CnchMergeTree(
                    order_by=func.tuple()
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table ('
            'x Int8, '
            'y Int8 MATERIALIZED x) '
            'ENGINE = CnchMergeTree() ORDER BY tuple()'
        )

    def test_create_table_column_alias(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int8),
            Column('dt', types.DateTime, bytehouse_alias=func.now()),
            engines.CnchMergeTree(
                order_by=func.tuple()
            )
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 ('
            'x Int8, '
            'dt DateTime ALIAS now()) '
            'ENGINE = CnchMergeTree() ORDER BY tuple()'
        )

    def test_create_table_column_all_defaults(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int8),
            Column(
                'dt', types.DateTime, server_default=func.now(),
                bytehouse_materialized=func.now(), bytehouse_alias=func.now()
            ),
            engines.CnchMergeTree(
                order_by=func.tuple()
            )
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 ('
            'x Int8, '
            'dt DateTime DEFAULT now()) '
            'ENGINE = CnchMergeTree() ORDER BY tuple()'
        )

    def test_add_column(self):
        col = Column(
            'x2', types.Int8, nullable=True, bytehouse_after=text('x1')
        )

        self.assertEqual(
            self.compile(CreateColumn(col)),
            'x2 Int8 AFTER x1'
        )

    def test_create_table_tuple(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Tuple(types.Int8, types.Float32)),
            engines.CnchMergeTree(
                order_by=func.tuple()
            )
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 ('
            'x Tuple(Int8, Float32)) '
            'ENGINE = CnchMergeTree() ORDER BY tuple()'
        )

    @require_server_version(21, 1, 3)
    def test_create_table_map(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Map(types.String, types.String)),
            engines.CnchMergeTree(
                order_by=func.tuple()
            )
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 ('
            'x Map(String, String)) '
            'ENGINE = CnchMergeTree() ORDER BY tuple()'
        )

    @unittest.skip("AssertionError: Lists differ: ['CRE[27 chars]t_cluster (x Int32) ENGINE = CnchMergeTree() Order "
                   "by tuple()'] != ['CRE[27 chars]t_cluster (x Int32) ENGINE =CnchMer...")
    def test_table_create_on_cluster(self):
        create_sql = (
            'CREATE TABLE t1 ON CLUSTER test_cluster '
            '(x Int32) ENGINE =CnchMergeTree() ORDER BY tuple()'
        )

        with mocked_engine() as engine:
            table = Table(
                't1', self.metadata(session=engine.session),
                Column('x', types.Int32, primary_key=True),
                engines.CnchMergeTree(
                    order_by=func.tuple()
                ),
                bytehouse_cluster='test_cluster'
            )

            table.create()
            self.assertEqual(engine.history, [create_sql])

        self.assertEqual(
            self.compile(CreateTable(table)),
            create_sql
        )

    def test_drop_table_clause(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True)
        )

        self.assertEqual(
            self.compile(DropTable(table)),
            'DROP TABLE t1'
        )
        self.assertEqual(
            self.compile(DropTable(table, if_exists=True)),
            'DROP TABLE IF EXISTS t1'
        )

    def test_table_drop(self):
        with mocked_engine() as engine:
            table = Table(
                't1', self.metadata(session=engine.session),
                Column('x', types.Int32, primary_key=True)
            )
            table.drop(if_exists=True)
            self.assertEqual(engine.history, ['DROP TABLE IF EXISTS t1'])

    def test_table_drop_on_cluster(self):
        drop_sql = 'DROP TABLE IF EXISTS t1 ON CLUSTER test_cluster'

        with mocked_engine() as engine:
            table = Table(
                't1', self.metadata(session=engine.session),
                Column('x', types.Int32, primary_key=True),
                bytehouse_cluster='test_cluster'
            )
            table.drop(if_exists=True)
            self.assertEqual(engine.history, [drop_sql])

        self.assertEqual(
            self.compile(DropTable(table, if_exists=True)),
            drop_sql
        )

    def test_create_all_drop_all(self):
        metadata = self.metadata(session=self.session)

        Table(
            't1', metadata,
            Column('x', types.Int32, primary_key=True),
            engines.CnchMergeTree(
                order_by=func.tuple()
            ),
        )

        metadata.create_all()
        metadata.drop_all()

    @unittest.skip("bytehouse_driver.packet.exception.ExceptionPacket: [code: 0, name: , message: Syntax error near: ...ITION BY toYYYYMM(date)")
    def test_create_drop_mat_view(self):
        Base = get_declarative_base(self.metadata())

        class Statistics(Base):
            date = Column(types.Date, primary_key=True)
            sign = Column(types.Int8, nullable=False)
            grouping = Column(types.Int32, nullable=False)
            metric1 = Column(types.Int32, nullable=False)

            __table_args__ = (
                engines.CnchMergeTree(
                    sign,
                    partition_by=func.toYYYYMM(date),
                    order_by=(date, grouping)
                ),
            )

        # Define storage for Materialized View
        class GroupedStatistics(Base):
            date = Column(types.Date, primary_key=True)
            metric1 = Column(types.Int32, nullable=False)

            __table_args__ = (
                engines.CnchMergeTree(
                    partition_by=func.toYYYYMM(date),
                    order_by=(date,)
                ),
            )

        # Define SELECT for Materialized View
        MatView = MaterializedView(GroupedStatistics, select([
            Statistics.date.label('date'),
            func.sum(Statistics.metric1 * Statistics.sign).label('metric1')
        ]).where(
            Statistics.grouping > 42
        ).group_by(
            Statistics.date
        ))

        Statistics.__table__.create()
        MatView.create()

        inspector = inspect(self.session.connection())

        self.assertTrue(inspector.has_table(MatView.name))
        MatView.drop()
        self.assertFalse(inspector.has_table(MatView.name))

    def test_create_table_with_comment(self):
        table = Table(
            't1', self.metadata(session=self.session),
            Column('x', types.Int32, primary_key=True),
            engines.CnchMergeTree(
                order_by=func.tuple()
            ),
            comment='table_comment'
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            "CREATE TABLE t1 (x Int32) ENGINE = CnchMergeTree() ORDER BY tuple() COMMENT 'table_comment'"
        )

    def test_create_table_with_column_comment(self):
        table = Table(
            't1', self.metadata(session=self.session),
            Column('x', types.Int32, primary_key=True, comment='col_comment'),
            engines.CnchMergeTree(
                order_by=func.tuple()
            )
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            "CREATE TABLE t1 (x Int32 COMMENT 'col_comment') ENGINE = CnchMergeTree() ORDER BY tuple()"
        )
