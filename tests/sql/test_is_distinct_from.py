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

# coding: utf-8

from parameterized import parameterized
from sqlalchemy import select, literal

from tests.testcase import BaseTestCase


class IsDistinctFromTestCase(BaseTestCase):
    def _select_bool(self, expr):
        query = select([expr])
        (result,), = self.session.execute(query).fetchall()
        return bool(result)

    @parameterized.expand([
        (1, 2),
        (1, None),
        (None, "NULL"),
        (None, u"ᴺᵁᴸᴸ"),
        ((1, None), (2, None)),
        ((1, (1, None)), (1, (2, None)))
    ])
    def test_is_distinct_from(self, a, b):
        self.assertTrue(self._select_bool(literal(a).is_distinct_from(b)))
        self.assertFalse(self._select_bool(literal(a).isnot_distinct_from(b)))

    @parameterized.expand([
        (None,),
        ((1, None),),
        ((None, None),),
        ((1, (1, None)),)
    ])
    def test_is_self_distinct_from(self, v):
        self.assertTrue(self._select_bool(literal(v).isnot_distinct_from(v)))
        self.assertFalse(self._select_bool(literal(v).is_distinct_from(v)))
