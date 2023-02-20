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

from sqlalchemy import Column, func

from bytehouse_sqlalchemy import engines, types, Table
from bytehouse_sqlalchemy.exceptions import DatabaseException
from tests.testcase import NativeSessionTestCase


class NativeInsertTestCase(NativeSessionTestCase):
    def test_rowcount_return(self):
        table = Table(
            'test', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            engines.CnchMergeTree(
                order_by=func.tuple()
            )
        )
        table.drop(if_exists=True)
        table.create()

        rv = self.session.execute(table.insert(), [{'x': x} for x in range(5)])
        self.assertEqual(rv.rowcount, 5)
        self.assertEqual(
            self.session.query(func.count()).select_from(table).scalar(), 5
        )

        rv = self.session.execute(
            'INSERT INTO test SELECT 5'
        )
        self.assertEqual(rv.rowcount, -1)

    @unittest.skip("Exception not catching DatabaseException")
    def test_types_check(self):
        table = Table(
            'test', self.metadata(),
            Column('x', types.UInt32, primary_key=True),
            engines.CnchMergeTree(
                order_by=func.tuple()
            )
        )
        table.drop(if_exists=True)
        table.create()

        with self.assertRaises(DatabaseException) as ex:
            self.session.execute(
                table.insert().execution_options(types_check=True),
                [{'x': -1}]
            )
        self.assertIn('-1 for column "x"', str(ex.exception.orig))

        with self.assertRaises(DatabaseException) as ex:
            self.session.execute(table.insert(), [{'x': -1}])
        self.assertIn(
            'Repeat query with types_check=True for detailed info',
            str(ex.exception.orig)
        )
