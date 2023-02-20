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

from sqlalchemy import Column, exc, delete

from bytehouse_sqlalchemy import types, Table, engines
from tests.testcase import NativeSessionTestCase
from tests.util import mock_object_attr


class DeleteTestCase(NativeSessionTestCase):
    def test_delete(self):
        t1 = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            engines.CnchMergeTree('x', order_by=('x',))
        )

        query = t1.delete().where(t1.c.x == 25)
        statement = self.compile(query, literal_binds=True)
        self.assertEqual(statement, 'ALTER TABLE t1 DELETE WHERE x = 25')

        query = delete(t1).where(t1.c.x == 25)
        statement = self.compile(query, literal_binds=True)
        self.assertEqual(statement, 'ALTER TABLE t1 DELETE WHERE x = 25')

    def test_delete_without_where(self):
        t1 = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            engines.CnchMergeTree('x', order_by=('x',))
        )

        query = t1.delete()
        with self.assertRaises(exc.CompileError) as ex:
            self.compile(query, literal_binds=True)

        self.assertEqual(str(ex.exception), 'WHERE clause is required')

        query = delete(t1)
        with self.assertRaises(exc.CompileError) as ex:
            self.compile(query, literal_binds=True)

        self.assertEqual(str(ex.exception), 'WHERE clause is required')

    def test_delete_unsupported(self):
        t1 = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            engines.CnchMergeTree('x', order_by=('x',))
        )
        t1.drop(if_exists=True)
        t1.create()

        with self.assertRaises(exc.CompileError) as ex:
            dialect = self.session.bind.dialect
            with mock_object_attr(dialect, 'supports_delete', False):
                self.session.execute(t1.delete().where(t1.c.x == 25))

        self.assertEqual(
            str(ex.exception),
            'ALTER DELETE is not supported by this server version'
        )

        with self.assertRaises(exc.CompileError) as ex:
            dialect = self.session.bind.dialect
            with mock_object_attr(dialect, 'supports_delete', False):
                self.session.execute(delete(t1).where(t1.c.x == 25))

        self.assertEqual(
            str(ex.exception),
            'ALTER DELETE is not supported by this server version'
        )
