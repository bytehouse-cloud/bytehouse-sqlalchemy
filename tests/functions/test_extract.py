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

"""
Test EXTRACT
"""
from sqlalchemy import Column, extract

from bytehouse_sqlalchemy import types
from tests.testcase import BaseTestCase


def get_date_column(name):
    """
    Return types.Date column
    :param name: Column name
    :return: sqlalchemy.Column
    """
    return Column(name, types.Date)


class ExtractTestCase(BaseTestCase):
    def test_extract_year(self):
        self.assertEqual(
            self.compile(extract('year', get_date_column('x'))),
            'toYear(x)'
        )

    def test_extract_month(self):
        self.assertEqual(
            self.compile(extract('month', get_date_column('x'))),
            'toMonth(x)'
        )

    def test_extract_day(self):
        self.assertEqual(
            self.compile(extract('day', get_date_column('x'))),
            'toDayOfMonth(x)'
        )

    def test_extract_unknown(self):
        self.assertEqual(
            self.compile(extract('test', get_date_column('x'))),
            'x'
        )
