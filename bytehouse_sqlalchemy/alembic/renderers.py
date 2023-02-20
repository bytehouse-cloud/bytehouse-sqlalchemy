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

from alembic.autogenerate import render
from alembic.autogenerate import renderers

from . import operations

indent = ' ' * 4


def escape(x):
    return x.replace("'", "\\'")


@renderers.dispatch_for(operations.CreateMatViewOp)
def render_create_mat_view(autogen_context, op):
    columns = [
        col
        for col in [
            render._render_column(col, autogen_context) for col in op.columns
        ]
        if col
    ]

    templ = (
        "{prefix}create_mat_view(\n"
        "{indent}'{name}',\n"
        "{indent}'{selectable}',\n"
        "{indent}'{engine}',\n"
        "{indent}{columns}\n"
        ")"
    )

    join_indent = ("'\n" + indent + "'")
    return templ.format(
        prefix=render._alembic_autogenerate_prefix(autogen_context),
        name=op.name,
        selectable=join_indent.join(escape(op.selectable).split('\n')),
        engine=join_indent.join(escape(op.engine.strip()).split('\n')),
        columns=(',\n' + indent).join(str(arg) for arg in columns),
        indent=indent
    )


@renderers.dispatch_for(operations.DropMatViewOp)
def render_drop_mat_view(autogen_context, op):
    return (
        render._alembic_autogenerate_prefix(autogen_context) +
        "drop_mat_view('" + op.name + "')"
    )


@renderers.dispatch_for(operations.CreateMatViewToTableOp)
def render_create_mat_view_to_table(autogen_context, op):
    templ = (
        "{prefix}create_mat_view_to_table(\n"
        "{indent}'{name}',\n"
        "{indent}'{selectable}',\n"
        "{indent}'{inner_name}'\n"
        ")"
    )

    join_indent = ("'\n" + indent + "'")
    return templ.format(
        prefix=render._alembic_autogenerate_prefix(autogen_context),
        name=op.name,
        selectable=join_indent.join(escape(op.selectable).split('\n')),
        inner_name=op.inner_name,
        indent=indent
    )


@renderers.dispatch_for(operations.DropMatViewToTableOp)
def render_drop_mat_view_to_table(autogen_context, op):
    return (
        render._alembic_autogenerate_prefix(autogen_context) +
        "drop_mat_view_to_table('" + op.name + "')"
    )


@renderers.dispatch_for(operations.AttachMatViewOp)
def render_attach_mat_view(autogen_context, op):
    columns = [
        col
        for col in [
            render._render_column(col, autogen_context) for col in op.columns
        ]
        if col
    ]

    templ = (
        "{prefix}attach_mat_view(\n"
        "{indent}'{name}',\n"
        "{indent}'{selectable}',\n"
        "{indent}'{engine}',\n"
        "{indent}{columns}\n"
        ")"
    )

    join_indent = ("'\n" + indent + "'")
    return templ.format(
        prefix=render._alembic_autogenerate_prefix(autogen_context),
        name=op.name,
        selectable=join_indent.join(escape(op.selectable).split('\n')),
        engine=join_indent.join(escape(op.engine.strip()).split('\n')),
        columns=(',\n' + indent).join(str(arg) for arg in columns),
        indent=indent
    )


@renderers.dispatch_for(operations.DetachMatViewOp)
def render_detach_mat_view(autogen_context, op):
    return (
        render._alembic_autogenerate_prefix(autogen_context) +
        "detach_mat_view('" + op.name + "')"
    )
