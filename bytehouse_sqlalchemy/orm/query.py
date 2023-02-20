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

from sqlalchemy import exc
from sqlalchemy.sql.base import _generative
from sqlalchemy.orm.query import Query as BaseQuery

from ..ext.clauses import (
    ArrayJoin,
    LeftArrayJoin,
    LimitByClause,
    sample_clause,
)


class Query(BaseQuery):
    _with_totals = False
    _final = None
    _sample = None
    _limit_by = None
    _array_join = None

    def _compile_context(self, *args, **kwargs):
        context = super(Query, self)._compile_context(*args, **kwargs)
        query = context.query

        query._with_totals = self._with_totals
        query._final_clause = self._final
        query._sample_clause = sample_clause(self._sample)
        query._limit_by_clause = self._limit_by
        query._array_join = self._array_join

        return context

    @_generative
    def with_totals(self):
        if not self._group_by_clauses:
            raise exc.InvalidRequestError(
                "Query.with_totals() can be used only with specified "
                "GROUP BY, call group_by()"
            )

        self._with_totals = True

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

    @_generative
    def final(self):
        self._final = True

    @_generative
    def sample(self, sample):
        self._sample = sample

    @_generative
    def limit_by(self, by_clauses, limit, offset=None):
        self._limit_by = LimitByClause(by_clauses, limit, offset)

    def join(self, *props, **kwargs):
        spec = {
            'type': kwargs.pop('type', None),
            'strictness': kwargs.pop('strictness', None),
            'distribution': kwargs.pop('distribution', None)
        }
        rv = super(Query, self).join(*props, **kwargs)
        for x in rv._legacy_setup_joins:
            x_spec = dict(spec)
            # use 'full' key to pass extra flags
            x_spec['full'] = x[-1]['full']
            x[-1]['full'] = x_spec
        return rv

    def outerjoin(self, *props, **kwargs):
        kwargs['type'] = kwargs.get('type') or 'LEFT OUTER'
        return self.join(*props, **kwargs)
