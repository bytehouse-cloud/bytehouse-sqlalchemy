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
from sqlalchemy import sql, Column, literal, literal_column

from bytehouse_sqlalchemy import types, Table, engines
from tests.testcase import CompilationTestCase, NativeSessionTestCase


class VisitTestCase(CompilationTestCase):
    def test_true_false(self):
        self.assertEqual(self.compile(sql.false()), '0')
        self.assertEqual(self.compile(sql.true()), '1')

    def test_array(self):
        self.assertEqual(
            self.compile(types.Array(types.Int32())),
            'Array(Int32)'
        )
        self.assertEqual(
            self.compile(types.Array(types.Array(types.Int32()))),
            'Array(Array(Int32))'
        )

    def test_enum(self):
        class MyEnum(enum.Enum):
            __order__ = 'foo bar'
            foo = 100
            bar = 500

        self.assertEqual(
            self.compile(types.Enum(MyEnum)),
            "Enum('foo' = 100, 'bar' = 500)"
        )

        self.assertEqual(
            self.compile(types.Enum16(MyEnum)),
            "Enum16('foo' = 100, 'bar' = 500)"
        )

        MyEnum = enum.Enum('MyEnum', [" ' t = ", "test"])

        self.assertEqual(
            self.compile(types.Enum8(MyEnum)),
            "Enum8(' \\' t = ' = 1, 'test' = 2)"
        )

    def test_do_not_allow_execution(self):
        with self.assertRaises(TypeError):
            self.session.execute('SHOW TABLES')

        with self.assertRaises(TypeError):
            self.session.query(literal(0)).all()


class VisitNativeTestCase(NativeSessionTestCase):
    def test_insert_no_templates_after_value(self):
        # Optimized non-templating insert test (native protocol only).
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32),
            engines.CnchMergeTree()
        )
        self.assertEqual(
            self.compile(table.insert()),
            'INSERT INTO t1 (x) VALUES'
        )

    def test_insert_inplace_values(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32),
            engines.CnchMergeTree()
        )
        self.assertEqual(
            self.compile(
                table.insert().values(x=literal_column(str(42))),
                literal_binds=True
            ), 'INSERT INTO t1 (x) VALUES (42)'
        )
