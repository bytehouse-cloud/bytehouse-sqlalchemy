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

from sqlalchemy import util, exc
from sqlalchemy.sql import type_api, roles
from sqlalchemy.sql.elements import (
    BindParameter,
    ColumnElement,
    ClauseList
)
from sqlalchemy.sql.util import _offset_or_limit_clause
from sqlalchemy.sql.visitors import Visitable


class SampleParam(BindParameter):
    pass


def sample_clause(element):
    """Convert the given value to an "sample" clause.

    This handles incoming element to an expression; if
    an expression is already given, it is passed through.

    """
    if element is None:
        return None
    elif hasattr(element, '__clause_element__'):
        return element.__clause_element__()
    elif isinstance(element, Visitable):
        return element
    else:
        return SampleParam(None, element, unique=True)


class LimitByClause:

    def __init__(self, by_clauses, limit, offset):
        self.by_clauses = ClauseList(
            *by_clauses, _literal_as_text=roles.ByOfRole
        )
        self.offset = _offset_or_limit_clause(offset)
        self.limit = _offset_or_limit_clause(limit)

    def __bool__(self):
        return bool(self.by_clauses.clauses)


class Lambda(ColumnElement):
    """Represent a lambda function, ``Lambda(lambda x: 2 * x)``."""

    __visit_name__ = 'lambda'

    def __init__(self, func):
        if not util.callable(func):
            raise exc.ArgumentError('func must be callable')

        self.type = type_api.NULLTYPE
        self.func = func


class ArrayJoin(ClauseList):
    __visit_name__ = 'array_join'


class LeftArrayJoin(ClauseList):
    __visit_name__ = 'left_array_join'
