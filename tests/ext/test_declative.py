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
from sqlalchemy.sql.ddl import CreateTable

from bytehouse_sqlalchemy import types, engines, get_declarative_base
from tests.testcase import BaseTestCase


class DeclarativeTestCase(BaseTestCase):
    def test_create_table(self):
        base = get_declarative_base()

        class TestTable(base):
            x = Column(types.Int32, primary_key=True)
            y = Column(types.String)

            __table_args__ = (
                engines.CnchMergeTree(
                    order_by=func.tuple()
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE test_table (x Int32, y String) ENGINE = CnchMergeTree() ORDER BY tuple()'
        )

    def test_create_table_custom_name(self):
        base = get_declarative_base()

        class TestTable(base):
            __tablename__ = 'testtable'

            x = Column(types.Int32, primary_key=True)
            y = Column(types.String)

            __table_args__ = (
                engines.CnchMergeTree(
                    order_by=func.tuple()
                ),
            )

        self.assertEqual(
            self.compile(CreateTable(TestTable.__table__)),
            'CREATE TABLE testtable (x Int32, y String) ENGINE = CnchMergeTree() ORDER BY tuple()'
        )
