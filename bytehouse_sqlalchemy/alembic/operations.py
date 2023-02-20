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

from alembic.operations import Operations, MigrateOperation
from alembic.operations.ops import ExecuteSQLOp


@Operations.register_operation('create_mat_view')
class CreateMatViewOp(MigrateOperation):
    def __init__(self, name, selectable, engine, *columns, **kwargs):
        self.name = name
        self.selectable = selectable
        self.engine = engine
        self.columns = columns
        self.kwargs = kwargs

    @classmethod
    def create_mat_view(cls, operations, name, selectable, engine, *columns,
                        **kwargs):
        """Issue a "CREATE MATERIALIZED VIEW" instruction."""

        op = CreateMatViewOp(name, selectable, engine, *columns, **kwargs)
        return operations.invoke(op)

    def reverse(self):
        return DropMatViewOp(
            self.name, self.selectable, self.engine, *self.columns,
            **self.kwargs
        )


@Operations.register_operation('create_mat_view_to_table')
class CreateMatViewToTableOp(MigrateOperation):
    def __init__(self, name, selectable, inner_name, **kwargs):
        self.name = name
        self.selectable = selectable
        self.inner_name = inner_name
        self.kwargs = kwargs

    @classmethod
    def create_mat_view_to_table(cls, operations, name, selectable, inner_name,
                                 **kwargs):
        """Issue a "CREATE MATERIALIZED VIEW" instruction wit "TO" clause."""

        op = CreateMatViewToTableOp(name, selectable, inner_name, **kwargs)
        return operations.invoke(op)

    def reverse(self):
        return DropMatViewToTableOp(
            self.name, self.selectable, self.inner_name, **self.kwargs
        )


@Operations.register_operation('drop_mat_view_to_table')
class DropMatViewToTableOp(MigrateOperation):
    def __init__(self, name, old_selectable, inner_name, **kwargs):
        self.name = name
        self.old_selectable = old_selectable
        self.inner_name = inner_name
        self.kwargs = kwargs

    @classmethod
    def drop_mat_view_to_table(cls, operations, name, **kwargs):
        """Issue a "DROP VIEW" instruction."""

        sql = 'DROP VIEW '
        if kwargs.get('if_exists'):
            sql += 'IF EXISTS '

        sql += name

        if kwargs.get('on_cluster'):
            sql += ' ON CLUSTER ' + kwargs['on_cluster']

        op = ExecuteSQLOp(sql)
        return operations.invoke(op)

    def reverse(self):
        return CreateMatViewToTableOp(
            self.name, self.old_selectable, self.inner_name, **self.kwargs
        )


@Operations.register_operation('drop_mat_view')
class DropMatViewOp(MigrateOperation):
    def __init__(self, name, selectable, engine, *columns, **kwargs):
        self.name = name
        self.selectable = selectable
        self.engine = engine
        self.columns = columns
        self.kwargs = kwargs

    @classmethod
    def drop_mat_view(cls, operations, name, **kwargs):
        """Issue a "DROP VIEW" instruction."""

        sql = 'DROP VIEW '
        if kwargs.get('if_exists'):
            sql += 'IF EXISTS '

        sql += name

        if kwargs.get('on_cluster'):
            sql += ' ON CLUSTER ' + kwargs['on_cluster']

        op = ExecuteSQLOp(sql)
        return operations.invoke(op)

    def reverse(self):
        return CreateMatViewOp(
            self.name, self.selectable, self.engine, *self.columns,
            **self.kwargs
        )


@Operations.register_operation('attach_mat_view')
class AttachMatViewOp(MigrateOperation):
    def __init__(self, name, selectable, engine, *columns, **kwargs):
        self.name = name
        self.selectable = selectable
        self.engine = engine
        self.columns = columns
        self.kwargs = kwargs

    @classmethod
    def attach_mat_view(cls, operations, name, selectable, engine, *columns,
                        **kwargs):
        """Issue a "ATTACH MATERIALIZED VIEW" instruction."""

        op = AttachMatViewOp(name, selectable, engine, *columns, **kwargs)
        return operations.invoke(op)

    def reverse(self):
        return DetachMatViewOp(
            self.name, self.selectable, self.engine, *self.columns,
            **self.kwargs
        )


@Operations.register_operation('detach_mat_view')
class DetachMatViewOp(MigrateOperation):
    def __init__(self, name, old_selectable, engine, *columns, **kwargs):
        self.name = name
        self.old_selectable = old_selectable
        self.engine = engine
        self.columns = columns
        self.kwargs = kwargs

    @classmethod
    def detach_mat_view(cls, operations, name, **kwargs):
        """Issue a "DETACH VIEW" instruction."""

        sql = 'DETACH VIEW '

        if kwargs.get('if_exists'):
            sql += 'IF EXISTS '

        sql += name

        if kwargs.get('on_cluster'):
            sql += ' ON CLUSTER ' + kwargs['on_cluster']

        if kwargs.get('permanently'):
            sql += ' PERMANENTLY'

        op = ExecuteSQLOp(sql)
        return operations.invoke(op)

    def reverse(self):
        return AttachMatViewOp(
            self.name, self.old_selectable, self.engine, *self.columns,
            **self.kwargs
        )
