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

from sqlalchemy import Column, exc

from bytehouse_sqlalchemy import types, Table
from tests.testcase import BaseTestCase
from tests.util import with_native_sessions


@with_native_sessions
class LimitTestCase(BaseTestCase):
    table = Table(
        't1', BaseTestCase.metadata(),
        Column('x', types.Int32, primary_key=True)
    )

    def test_limit(self):
        query = self.session.query(self.table.c.x).limit(10)
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT t1.x AS t1_x FROM t1  LIMIT 10'
        )

    def test_limit_with_offset(self):
        query = self.session.query(self.table.c.x).limit(10).offset(5)
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT t1.x AS t1_x FROM t1  LIMIT 5, 10'
        )

        query = self.session.query(self.table.c.x).offset(5).limit(10)
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT t1.x AS t1_x FROM t1  LIMIT 5, 10'
        )

    def test_offset_without_limit(self):
        with self.assertRaises(exc.CompileError) as ex:
            query = self.session.query(self.table.c.x).offset(5)
            self.compile(query, literal_binds=True)

        self.assertEqual(
            str(ex.exception),
            'OFFSET without LIMIT is not supported'
        )
