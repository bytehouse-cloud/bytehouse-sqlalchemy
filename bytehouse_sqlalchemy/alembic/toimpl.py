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

from alembic.operations import Operations
from sqlalchemy.sql.ddl import CreateColumn

from . import operations


@Operations.implementation_for(operations.CreateMatViewOp)
def create_mat_view(operations, operation):
    impl = operations.impl
    ddl_compiler = impl.dialect.ddl_compiler(impl.dialect, None)

    text = 'CREATE MATERIALIZED VIEW '

    if operation.kwargs.get('if_not_exists'):
        text += 'IF NOT EXISTS '

    text += operation.name

    if operation.kwargs.get('on_cluster'):
        text += ' ON CLUSTER ' + operation.kwargs['on_cluster']

    text += ' (' + ', '.join(
        ddl_compiler.process(CreateColumn(c)) for c in operation.columns
    ) + ') '

    text += 'ENGINE = ' + operation.engine

    if operation.kwargs.get('populate'):
        text += ' POPULATE'

    text += ' AS ' + operation.selectable

    operations.execute(text)


@Operations.implementation_for(operations.CreateMatViewToTableOp)
def create_mat_view_to_table(operations, operation):
    text = 'CREATE MATERIALIZED VIEW '

    if operation.kwargs.get('if_not_exists'):
        text += 'IF NOT EXISTS '

    text += operation.name

    if operation.kwargs.get('on_cluster'):
        text += ' ON CLUSTER ' + operation.kwargs['on_cluster']

    text += ' TO ' + operation.inner_name

    if operation.kwargs.get('populate'):
        text += ' POPULATE'

    text += ' AS ' + operation.selectable

    operations.execute(text)


@Operations.implementation_for(operations.AttachMatViewOp)
def attach_mat_view(operations, operation):
    impl = operations.impl
    ddl_compiler = impl.dialect.ddl_compiler(impl.dialect, None)

    text = 'ATTACH MATERIALIZED VIEW '

    if operation.kwargs.get('if_not_exists'):
        text += 'IF NOT EXISTS '

    text += operation.name + ' '

    if operation.kwargs.get('on_cluster'):
        text += ' ON CLUSTER ' + operation.kwargs['on_cluster']

    text += ' (' + ', '.join(
        ddl_compiler.process(CreateColumn(c)) for c in operation.columns
    ) + ') '

    text += 'ENGINE = ' + operation.engine + ' AS ' + operation.selectable

    operations.execute(text)
