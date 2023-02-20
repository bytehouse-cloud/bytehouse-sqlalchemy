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

from contextlib import contextmanager
from functools import wraps

from parameterized import parameterized_class

from tests.session import native_session


def skip_by_server_version(testcase, version_required):
    testcase.skipTest(
        'Mininum revision required: {}'.format(
            '.'.join(str(x) for x in version_required)
        )
    )


def require_server_version(*version_required):
    def check(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            self = args[0]
            conn = self.session.bind.raw_connection()

            dialect = self.session.bind.dialect.name
            if dialect == 'bytehouse':
                # i = conn.transport.connection.context.server_info
                # current = (i.version_major, i.version_minor, i.version_patch)
                current = (0, 1, 54406)
            else:
                # cursor = conn.cursor()
                # cursor.execute(
                #     'SELECT version()'
                # )
                # version = cursor.fetchall()[0][0].split('.')
                version = (0, 1, 54406)
                current = tuple(int(x) for x in version[:3])

            conn.close()
            if version_required <= current:
                return f(*args, **kwargs)
            else:
                self.skipTest(
                    'Mininum revision required: {}'.format(
                        '.'.join(str(x) for x in version_required)
                    )
                )

        return wrapper

    return check


@contextmanager
def mock_object_attr(dialect, attr, new_value):
    old_value = getattr(dialect, attr)
    setattr(dialect, attr, new_value)

    try:
        yield
    finally:
        setattr(dialect, attr, old_value)


def class_name_func(cls, num, params_dict):
    suffix = 'Native'
    return cls.__name__ + suffix


with_native_sessions = parameterized_class([
    {'session': native_session}
], class_name_func=class_name_func)
