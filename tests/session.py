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

from sqlalchemy import create_engine

from bytehouse_sqlalchemy import make_session
from tests.config import native_uri, system_native_uri

native_engine = create_engine(native_uri)
native_session = make_session(native_engine)

system_native_session = make_session(create_engine(system_native_uri))


class MockedEngine(object):

    prev_do_execute = None
    prev_do_executemany = None
    prev_get_server_version_info = None
    prev_get_default_schema_name = None

    def __init__(self, session=None):
        self._buffer = []

        if session is None:
            session = make_session(create_engine('bytehouse:///?'))

        self.session = session
        self.dialect_cls = session.bind.dialect.__class__

    @property
    def history(self):
        return [re.sub(r'[\n\t]', '', str(ssql)) for ssql in self._buffer]

    def __enter__(self):
        self.prev_do_execute = self.dialect_cls.do_execute
        self.prev_do_executemany = self.dialect_cls.do_executemany
        self.prev_get_server_version_info = \
            self.dialect_cls._get_server_version_info
        self.prev_get_default_schema_name = \
            self.dialect_cls._get_default_schema_name

        def do_executemany(
                instance, cursor, statement, parameters, context=None):
            self._buffer.append(statement)

        def do_execute(instance, cursor, statement, parameters, context=None):
            self._buffer.append(statement)

        def get_server_version_info(*args, **kwargs):
            return (19, 16, 2, 2)

        def get_default_schema_name(*args, **kwargs):
            return 'test'

        self.dialect_cls.do_execute = do_execute
        self.dialect_cls.do_executemany = do_executemany
        self.dialect_cls._get_server_version_info = get_server_version_info
        self.dialect_cls._get_default_schema_name = get_default_schema_name

        return self

    def __exit__(self, *exc_info):
        self.dialect_cls.do_execute = self.prev_do_execute
        self.dialect_cls.do_executemany = self.prev_do_executemany
        self.dialect_cls._get_server_version_info = \
            self.prev_get_server_version_info
        self.dialect_cls._get_default_schema_name = \
            self.prev_get_default_schema_name


mocked_engine = MockedEngine
