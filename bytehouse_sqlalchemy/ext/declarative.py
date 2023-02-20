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

import re

from sqlalchemy import Column
from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base

from ..sql.schema import Table


class ByteHouseDeclarativeMeta(DeclarativeMeta):
    """
    Generates __tablename__ automatically. Taken from flask-sqlalchemy.
    Also adds custom __table_cls__.
    """
    _camelcase_re = re.compile(r'([A-Z]+)(?=[a-z0-9])')

    def __new__(cls, name, bases, d):
        tablename = d.get('__tablename__')

        has_pks = any(
            v.primary_key for k, v in d.items() if isinstance(v, Column)
        )

        # generate a table name automatically if it's missing and the
        # class dictionary declares a primary key.  We cannot always
        # attach a primary key to support model inheritance that does
        # not use joins.  We also don't want a table name if a whole
        # table is defined
        if not tablename and d.get('__table__') is None and has_pks:
            def _join(match):
                word = match.group()
                if len(word) > 1:
                    return ('_%s_%s' % (word[:-1], word[-1])).lower()
                return '_' + word.lower()
            d['__tablename__'] = cls._camelcase_re.sub(_join, name).lstrip('_')

        if '__table_cls__' not in d:
            d['__table_cls__'] = Table

        return DeclarativeMeta.__new__(cls, name, bases, d)


def get_declarative_base(metadata=None):
    return declarative_base(
        metadata=metadata, metaclass=ByteHouseDeclarativeMeta
    )
