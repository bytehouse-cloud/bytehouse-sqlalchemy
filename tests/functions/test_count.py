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

from sqlalchemy import Column, func

from bytehouse_sqlalchemy import types, Table
from tests.testcase import CompilationTestCase


class CountTestCaseBase(CompilationTestCase):
    table = Table(
        't1', CompilationTestCase.metadata(),
        Column('x', types.Int32, primary_key=True)
    )

    def test_count(self):
        self.assertEqual(
            self.compile(self.session.query(func.count(self.table.c.x))),
            'SELECT count(t1.x) AS count_1 FROM t1'
        )

    def test_count_distinct(self):
        query = self.session.query(func.count(func.distinct(self.table.c.x)))
        self.assertEqual(
            self.compile(query),
            'SELECT count(distinct(t1.x)) AS count_1 FROM t1'
        )

    def test_count_no_column_specified(self):
        query = self.session.query(func.count()).select_from(self.table)
        self.assertEqual(
            self.compile(query),
            'SELECT count(*) AS count_1 FROM t1'
        )
