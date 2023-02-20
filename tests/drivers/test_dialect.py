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

import unittest

from sqlalchemy import Column, create_engine, inspect, func

from bytehouse_sqlalchemy import make_session, engines, types, Table
from tests.testcase import BaseTestCase
from tests.config import (
    system_native_uri,
    database as test_database,
)
from tests.util import with_native_sessions, require_server_version


@with_native_sessions
class ByteHouseDialectTestCase(BaseTestCase):

    @property
    def dialect(self):
        return self.session.bind.dialect

    @property
    def connection(self):
        return self.session.connection()

    def setUp(self):
        super(ByteHouseDialectTestCase, self).setUp()
        self.table = Table(
            'test_exists_table',
            self.metadata(),
            Column('x', types.Int32, primary_key=True),
            engines.CnchMergeTree(
                order_by=func.tuple()
            )
        )
        self.table.drop(self.session.bind, if_exists=True)

    def tearDown(self):
        self.table.drop(self.session.bind, if_exists=True)
        super(ByteHouseDialectTestCase, self).tearDown()

    def test_has_table(self):
        self.assertFalse(
            self.dialect.has_table(self.session,
                                   self.table.name)
        )

        self.table.create(self.session.bind)
        self.assertTrue(
            self.dialect.has_table(self.session,
                                   self.table.name)
        )

    def test_has_table_with_schema(self):
        self.assertFalse(
            self.dialect.has_table(self.session, 'bad', schema=self.database)
        )
        self.assertFalse(
            self.dialect.has_table(self.session, 'columns', schema=self.database)
        )

        self.table.drop(self.session.bind, if_exists=True)
        self.table.create(self.session.bind)
        self.assertTrue(
            self.dialect.has_table(self.session, 'test_exists_table', schema=self.database)
        )

    def test_get_table_names(self):
        self.table.create(self.session.bind)
        db_tables = self.dialect.get_table_names(self.connection)
        self.assertIn(self.table.name, db_tables)

    def test_get_table_names_with_schema(self):
        self.table.create(self.session.bind)
        db_tables = self.dialect.get_table_names(self.connection, self.database)
        self.assertIn(self.table.name, db_tables)

    def test_get_view_names(self):
        self.table.create(self.session.bind)
        db_views = self.dialect.get_view_names(self.connection)
        self.assertNotIn(self.table.name, db_views)

    def test_get_view_names_with_schema(self):
        self.table.create(self.session.bind)
        db_views = self.dialect.get_view_names(self.connection, test_database)
        self.assertNotIn(self.table.name, db_views)

    def test_reflecttable(self):
        self.table.create(self.session.bind)
        meta = self.metadata()
        insp = inspect(self.session.bind)
        reflected_table = Table(self.table.name, meta)
        insp.reflecttable(reflected_table, None)

        self.assertEqual(self.table.name, reflected_table.name)

    def test_get_schema_names(self):
        schemas = self.dialect.get_schema_names(self.session)
        self.assertIn(test_database, schemas)

    def test_columns_compilation(self):
        # should not raise UnsupportedCompilationError
        col = Column('x', types.Nullable(types.Int32))
        self.assertEqual(str(col.type), 'Nullable(Int32)')

