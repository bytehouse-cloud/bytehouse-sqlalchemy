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

from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.schema import ColumnCollectionMixin, SchemaItem, Constraint


class Engine(Constraint):
    __visit_name__ = 'engine'

    def __init__(self, *args, **kwargs):
        pass

    def get_parameters(self):
        return []

    def extend_parameters(self, *params):
        rv = []
        for param in params:
            if isinstance(param, (tuple, list)):
                rv.extend(param)
            elif param is not None:
                rv.append(param)
        return rv

    @property
    def name(self):
        return self.__class__.__name__

    def _set_parent(self, parent, **kwargs):
        self.parent = parent
        parent.engine = self

    @classmethod
    def reflect(cls, table, engine_full, **kwargs):
        raise NotImplementedError


class TableCol(ColumnCollectionMixin, SchemaItem):
    def __init__(self, column, **kwargs):
        super(TableCol, self).__init__(*[column], **kwargs)

    def get_column(self):
        return list(self.columns)[0]


class KeysExpressionOrColumn(ColumnCollectionMixin, SchemaItem):
    def __init__(self, *expressions, **kwargs):
        self.expressions = []

        super(KeysExpressionOrColumn, self).__init__(
            *expressions, _gather_expressions=self.expressions, **kwargs
        )

    def _set_parent(self, table, **kw):
        ColumnCollectionMixin._set_parent(self, table)

        self.table = table

        expressions = self.expressions
        col_expressions = self._col_expressions(table)
        assert len(expressions) == len(col_expressions)
        self.expressions = [
            expr if isinstance(expr, ClauseElement) else colexpr
            for expr, colexpr in zip(expressions, col_expressions)
        ]

    def get_expressions_or_columns(self):
        return self.expressions
