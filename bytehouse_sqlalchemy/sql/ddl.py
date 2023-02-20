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

from sqlalchemy.sql.ddl import (
    SchemaDropper as SchemaDropperBase, DropTable as DropTableBase,
    SchemaGenerator as SchemaGeneratorBase, _CreateDropBase
)
from sqlalchemy.sql.expression import UnaryExpression
from sqlalchemy.sql.operators import custom_op


class DropTable(DropTableBase):
    def __init__(self, element, bind=None, if_exists=False):
        self.on_cluster = element.dialect_options['bytehouse']['cluster']
        super(DropTable, self).__init__(element, bind=bind,
                                        if_exists=if_exists)


class DropView(DropTableBase):
    def __init__(self, element, bind=None, if_exists=False):
        self.on_cluster = element.cluster
        super(DropView, self).__init__(element, bind=bind, if_exists=if_exists)


class SchemaDropper(SchemaDropperBase):
    def __init__(self, dialect, connection, if_exists=False, **kwargs):
        self.if_exists = if_exists
        super(SchemaDropper, self).__init__(dialect, connection, **kwargs)

    def visit_table(self, table, **kwargs):
        self.connection.execute(DropTable(table, if_exists=self.if_exists))

    def visit_materialized_view(self, table, **kwargs):
        self.connection.execute(DropView(table, if_exists=self.if_exists))


class CreateMaterializedView(_CreateDropBase):
    """Represent a CREATE MATERIALIZED VIEW statement."""

    __visit_name__ = "create_materialized_view"

    def __init__(self, element, if_not_exists=False):
        self.if_not_exists = if_not_exists
        super(CreateMaterializedView, self).__init__(element)


class SchemaGenerator(SchemaGeneratorBase):
    def __init__(self, dialect, connection, if_not_exists=False, **kwargs):
        self.if_not_exists = if_not_exists
        super(SchemaGenerator, self).__init__(dialect, connection, **kwargs)

    def visit_materialized_view(self, table, **kwargs):
        self.connection.execute(
            CreateMaterializedView(table, if_not_exists=self.if_not_exists)
        )


def ttl_delete(expr):
    return UnaryExpression(expr, modifier=custom_op('DELETE'))


def ttl_to_disk(expr, disk):
    assert isinstance(disk, str), 'Disk must be str'
    return expr.op('TO DISK')(disk)


def ttl_to_volume(expr, volume):
    assert isinstance(volume, str), 'Volume must be str'
    return expr.op('TO VOLUME')(volume)
