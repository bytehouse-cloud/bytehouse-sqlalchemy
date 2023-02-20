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
from contextlib import contextmanager
from unittest import TestCase

from sqlalchemy import MetaData
from sqlalchemy.orm import Query

from tests.config import database, region, user, password, account
from tests.session import native_session, system_native_session
from tests.util import skip_by_server_version


class BaseAbstractTestCase(object):
    """ Supporting code for tests """
    required_server_version = None
    server_version = None

    region = region
    database = database
    user = user
    password = password
    account = account

    strip_spaces = re.compile(r'[\n\t]')
    session = native_session

    @classmethod
    def metadata(cls, session=None):
        if session is None:
            session = cls.session
        return MetaData(bind=session.bind)

    def _compile(self, clause, bind=None, literal_binds=False,
                 render_postcompile=False):
        if bind is None:
            bind = self.session.bind
        if isinstance(clause, Query):
            context = clause._compile_context()
            clause = context.query

        kw = {}
        compile_kwargs = {}
        if literal_binds:
            compile_kwargs['literal_binds'] = True
        if render_postcompile:
            compile_kwargs['render_postcompile'] = True

        if compile_kwargs:
            kw['compile_kwargs'] = compile_kwargs

        return clause.compile(dialect=bind.dialect, **kw)

    def compile(self, clause, **kwargs):
        return self.strip_spaces.sub(
            '', str(self._compile(clause, **kwargs))
        )

    @contextmanager
    def create_table(self, table):
        table.drop(bind=self.session.bind, if_exists=True)
        table.create(bind=self.session.bind)
        try:
            yield
        finally:
            table.drop(bind=self.session.bind)


class BaseTestCase(BaseAbstractTestCase, TestCase):
    """ Actually tests """

    @classmethod
    def setUpClass(cls):
        # System database is always present.
        system_native_session.execute(
            'DROP DATABASE IF EXISTS {}'.format(cls.database)
        )
        system_native_session.execute(
            'CREATE DATABASE {}'.format(cls.database)
        )

        # version = system_native_session.execute('SELECT version()').fetchall()
        # cls.server_version = tuple(int(x) for x in version[0][0].split('.'))
        cls.server_version = (0, 1, 54406)

        super(BaseTestCase, cls).setUpClass()

    def setUp(self):
        super(BaseTestCase, self).setUp()

        required = self.required_server_version

        if required and required > self.server_version:
            skip_by_server_version(self, self.required_server_version)


class NativeSessionTestCase(BaseTestCase):
    """ Explicitly Native-protocol-based session Test Case """

    session = native_session


class CompilationTestCase(BaseTestCase):
    """ Test Case that should be used only for SQL generation """

    session = native_session

    @classmethod
    def setUpClass(cls):
        super(CompilationTestCase, cls).setUpClass()

        cls._session_connection = cls.session.connection
        cls._session_execute = cls.session.execute

        cls.session.execute = None
        cls.session.connection = None

    @classmethod
    def tearDownClass(cls):
        cls.session.execute = cls._session_execute
        cls.session.connection = cls._session_connection

        super(CompilationTestCase, cls).tearDownClass()
