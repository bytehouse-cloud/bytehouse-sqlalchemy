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

from decimal import Decimal

from sqlalchemy import Column, Numeric, func
from sqlalchemy.sql.ddl import CreateTable

from bytehouse_sqlalchemy import types, engines, Table
from tests.testcase import (
    BaseTestCase, CompilationTestCase,
    NativeSessionTestCase
)


class NumericCompilationTestCase(CompilationTestCase):
    table = Table(
        'test', CompilationTestCase.metadata(),
        Column('x', Numeric(10, 2)),
        engines.CnchMergeTree(
            order_by=func.tuple()
        )
    )

    def test_create_table(self):
        self.assertEqual(
            self.compile(CreateTable(self.table)),
            'CREATE TABLE test (x Decimal(10, 2)) ENGINE = CnchMergeTree() ORDER BY tuple()'
        )

    def test_create_table_decimal_symlink(self):
        table = Table(
            'test', CompilationTestCase.metadata(),
            Column('x', types.Decimal(10, 2)),
            engines.CnchMergeTree(
                order_by=func.tuple()
            ))

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE test (x Decimal(10, 2)) ENGINE = CnchMergeTree() ORDER BY tuple()'
        )


class NumericTestCase(BaseTestCase):
    table = Table(
        'test', BaseTestCase.metadata(),
        Column('x', Numeric(10, 2)),
        engines.CnchMergeTree(
            order_by=func.tuple()
        )
    )

    def test_select_insert(self):
        x = Decimal('12345678.12')

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{'x': x}])
            self.assertEqual(self.session.query(self.table.c.x).scalar(), x)


class NumericNativeTestCase(NativeSessionTestCase):
    table = Table(
        'test', NativeSessionTestCase.metadata(),
        Column('x', Numeric(10, 2)),
        engines.CnchMergeTree(
            order_by=func.tuple()
        )
    )

    def test_insert_truncate(self):
        value = Decimal('123.129999')
        expected = Decimal('123.12')

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{'x': value}])
            self.assertEqual(self.session.query(self.table.c.x).scalar(),
                             expected)
