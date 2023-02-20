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

from sqlalchemy import Column, func, exc, text
from sqlalchemy.sql.ddl import CreateTable

from bytehouse_sqlalchemy import types, engines, get_declarative_base, Table
from bytehouse_sqlalchemy.sql.ddl import (
    ttl_delete,
    ttl_to_disk,
    ttl_to_volume,
)
from tests.testcase import CompilationTestCase


class EngineTestCaseBase(CompilationTestCase):
    @property
    def base(self):
        return get_declarative_base()


class GenericEngineTestCase(EngineTestCaseBase):
    def test_text_engine_columns_declarative(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)

            __table_args__ = (
                engines.CnchMergeTree(
                    partition_by='date',
                    order_by=('date', 'x')
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32, y String) '
            'ENGINE = CnchMergeTree() '
            'PARTITION BY date '
            'ORDER BY (date, x)'
        )

    def test_text_engine_columns(self):
        table = Table(
            't1', self.metadata(),
            Column('date', types.Date, primary_key=True),
            Column('x', types.Int32),
            Column('y', types.String),
            engines.CnchMergeTree(
                partition_by='date',
                order_by=('date', 'x')
            ),
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 (date Date, x Int32, y String) '
            'ENGINE = CnchMergeTree() '
            'PARTITION BY date '
            'ORDER BY (date, x)'
        )

    def test_func_engine_columns(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)

            __table_args__ = (
                engines.CnchMergeTree(
                    partition_by='date',
                    order_by=('date', func.intHash32(x)),
                    sample_by=func.intHash32(x)
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32, y String) '
            'ENGINE = CnchMergeTree() PARTITION BY date '
            'ORDER BY (date, intHash32(x)) '
            'SAMPLE BY intHash32(x)'
        )

    def test_create_table_without_engine(self):
        no_engine_table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.String)
        )

        with self.assertRaises(exc.CompileError) as ex:
            self.compile(CreateTable(no_engine_table))

        self.assertEqual(str(ex.exception), "No engine for table 't1'")

    def test_multiple_columns_partition_by(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)

            __table_args__ = (
                engines.CnchMergeTree(
                    partition_by=(date, x),
                    order_by='date'
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32, y String) '
            'ENGINE = CnchMergeTree() PARTITION BY (date, x) '
            'ORDER BY date'
        )


class MergeTreeTestCase(EngineTestCaseBase):

    def test_all_settings(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)
            y = Column(types.String)

            __table_args__ = (
                engines.CnchMergeTree(
                    partition_by=date,
                    order_by=(date, x),
                    primary_key=(x, y),
                    sample_by=func.hashFunc(x),
                    setting1=2,
                    setting2=5
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table '
            '(date Date, x Int32, y String) '
            'ENGINE = CnchMergeTree() '
            'PARTITION BY date '
            'ORDER BY (date, x) '
            'PRIMARY KEY (x, y) '
            'SAMPLE BY hashFunc(x) '
            'SETTINGS setting1=2, setting2=5'
        )


class MiscEnginesTestCase(EngineTestCaseBase):

    def test_cnch_merge_tree(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)

            __table_args__ = (
                engines.CnchMergeTree(
                    order_by=func.tuple()
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32) '
            'ENGINE = CnchMergeTree() ORDER BY tuple()'
        )


class TTLTestCase(EngineTestCaseBase):
    def test_ttl_expr(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)

            __table_args__ = (
                engines.CnchMergeTree(
                    ttl=date + func.toIntervalDay(1),
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32) '
            'ENGINE = CnchMergeTree() '
            'TTL date + toIntervalDay(1)'
        )

    def test_ttl_delete(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)

            __table_args__ = (
                engines.CnchMergeTree(
                    ttl=ttl_delete(date + func.toIntervalDay(1)),
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32) '
            'ENGINE = CnchMergeTree() '
            'TTL date + toIntervalDay(1) DELETE'
        )

    def test_ttl_list(self):
        class TestTable(self.base):
            date = Column(types.Date, primary_key=True)
            x = Column(types.Int32)

            __table_args__ = (
                engines.CnchMergeTree(
                    ttl=[
                        ttl_delete(date + func.toIntervalDay(1)),
                        ttl_to_disk(date + func.toIntervalDay(1), 'hdd'),
                        ttl_to_volume(date + func.toIntervalDay(1), 'slow'),
                    ],
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (date Date, x Int32) '
            'ENGINE = CnchMergeTree() '
            'TTL date + toIntervalDay(1) DELETE, '
            '    date + toIntervalDay(1) TO DISK \'hdd\', '
            '    date + toIntervalDay(1) TO VOLUME \'slow\''
        )
