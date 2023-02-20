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

from sqlalchemy import func, Column, types as sqltypes

try:
    from alembic.ddl import impl
    from alembic.ddl.base import (
        compiles, ColumnComment, format_table_name, format_column_name
    )
except ImportError:
    raise RuntimeError('alembic must be installed')

from bytehouse_sqlalchemy import types, engines
from bytehouse_sqlalchemy.sql.ddl import DropTable
from .comparators import compare_mat_view
from .renderers import (
    render_attach_mat_view, render_detach_mat_view,
    render_create_mat_view, render_drop_mat_view
)
from .toimpl import (
    create_mat_view, attach_mat_view
)


class ByteHouseDialectImpl(impl.DefaultImpl):
    __dialect__ = 'bytehouse'
    transactional_ddl = False

    def drop_table(self, table):
        self._exec(DropTable(table))


def patch_alembic_version(context, **kwargs):
    migration_context = context._proxy._migration_context
    version = migration_context._version

    dt = Column('dt', types.DateTime, server_default=func.now())
    version_num = Column('version_num', types.String, primary_key=True)
    version.append_column(dt)
    version.append_column(version_num)

    if 'cluster' in kwargs:
        cluster = kwargs['cluster']
        version.engine = engines.ReplicatedReplacingMergeTree(
            kwargs['table_path'], kwargs['replica_name'],
            version=dt, order_by=func.tuple()
        )
        version.kwargs['bytehouse_cluster'] = cluster
    else:
        version.engine = engines.ReplacingMergeTree(
            version=dt, order_by=func.tuple()
        )


def include_object(object, name, type_, reflected, compare_to):
    # skip inner matview tables in autogeneration.
    if type_ == 'table' and object.info.get('mv_storage'):
        return False

    return True


@compiles(ColumnComment, 'bytehouse')
def visit_column_comment(element, compiler, **kw):
    ddl = "ALTER TABLE {table_name} COMMENT COLUMN {column_name} {comment}"
    comment = (
        compiler.sql_compiler.render_literal_value(
            element.comment or '', sqltypes.String()
        )
    )

    return ddl.format(
        table_name=format_table_name(
            compiler, element.table_name, element.schema
        ),
        column_name=format_column_name(compiler, element.column_name),
        comment=comment,
    )


__all__ = (
    'ByteHouseDialectImpl', 'compare_mat_view',
    'render_attach_mat_view', 'render_detach_mat_view',
    'render_create_mat_view', 'render_drop_mat_view',
    'create_mat_view', 'attach_mat_view'
)
