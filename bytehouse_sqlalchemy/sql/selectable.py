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

from sqlalchemy.sql.base import _generative
from sqlalchemy.sql.selectable import (
    Select as StandardSelect,
    Join
)

from ..ext.clauses import (
    ArrayJoin,
    LeftArrayJoin,
    LimitByClause,
    sample_clause,
)


__all__ = ('Select', 'select')


class Select(StandardSelect):
    _with_totals = False
    _final_clause = None
    _sample_clause = None
    _limit_by_clause = None
    _array_join = None

    @_generative
    def with_totals(self):
        self._with_totals = True

    @_generative
    def final(self):
        self._final_clause = True

    @_generative
    def sample(self, sample):
        self._sample_clause = sample_clause(sample)

    @_generative
    def limit_by(self, by_clauses, limit, offset=None):
        self._limit_by_clause = LimitByClause(by_clauses, limit, offset)

    def _add_array_join(self, columns, left):
        join_type = ArrayJoin if not left else LeftArrayJoin
        self._array_join = join_type(*columns)

    @_generative
    def array_join(self, *columns, **kwargs):
        left = kwargs.get("left", False)
        self._add_array_join(columns, left=left)

    @_generative
    def left_array_join(self, *columns):
        self._add_array_join(columns, left=True)

    def join(self, right, onclause=None, isouter=False, full=False, type=None,
             strictness=None, distribution=None):
        flags = {
            'full': full,
            'type': type,
            'strictness': strictness,
            'distribution': distribution
        }
        return Join(self, right, onclause=onclause, isouter=isouter,
                    full=flags)


select = Select._create
