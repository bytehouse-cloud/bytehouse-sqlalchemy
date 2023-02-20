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

from sqlalchemy.util import asbool

from . import connector
from ..base import (
    ByteHouseDialect, ByteHouseExecutionContextBase, ByteHouseSQLCompiler,
)

# Export connector version
VERSION = (0, 0, 2, None)


class ByteHouseExecutionContext(ByteHouseExecutionContextBase):
    def pre_exec(self):
        # Always do executemany on INSERT with VALUES clause.
        if self.isinsert and self.compiled.statement.select is None:
            self.executemany = True


class ByteHouseNativeSQLCompiler(ByteHouseSQLCompiler):

    def visit_insert(self, insert_stmt, asfrom=False, **kw):
        rv = super(ByteHouseNativeSQLCompiler, self).visit_insert(
            insert_stmt, asfrom=asfrom, **kw)

        if kw.get('literal_binds') or insert_stmt._values:
            return rv

        pos = rv.lower().rfind('values (')
        # Remove (%s)-templates from VALUES clause if exists.
        # ClickHouse server since version 19.3.3 parse query after VALUES and
        # allows inplace parameters.
        # Example: INSERT INTO test (x) VALUES (1), (2).
        if pos != -1:
            rv = rv[:pos + 6]
        return rv


class ByteHouseDialect_native(ByteHouseDialect):
    driver = 'native'
    execution_ctx_cls = ByteHouseExecutionContext
    statement_compiler = ByteHouseNativeSQLCompiler

    supports_statement_cache = True

    @classmethod
    def dbapi(cls):
        return connector

    def create_connect_args(self, url):
        url = url.set(drivername='bytehouse')

        self.engine_reflection = asbool(
            url.query.get('engine_reflection', 'true')
        )

        return (str(url), ), {}

    def _execute(self, connection, sql, scalar=False, **kwargs):
        f = connection.scalar if scalar else connection.execute
        return f(sql, **kwargs)


dialect = ByteHouseDialect_native
