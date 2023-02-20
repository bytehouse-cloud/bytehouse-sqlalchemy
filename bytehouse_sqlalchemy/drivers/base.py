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

import enum

from sqlalchemy import schema, types as sqltypes, util as sa_util, text
from sqlalchemy.engine import default, reflection
from sqlalchemy.sql import (
    compiler, elements
)
from sqlalchemy.types import DATE, DATETIME, FLOAT
from sqlalchemy.util import (
    warn,
)

from .compilers.ddlcompiler import ByteHouseDDLCompiler
from .compilers.sqlcompiler import ByteHouseSQLCompiler
from .compilers.typecompiler import ByteHouseTypeCompiler
from .reflection import ByteHouseInspector
from .. import types

# Column specifications
colspecs = {}


# Type converters
ischema_names = {
    'Int256': types.Int256,
    'Int128': types.Int128,
    'Int64': types.Int64,
    'Int32': types.Int32,
    'Int16': types.Int16,
    'Int8': types.Int8,
    'UInt256': types.UInt256,
    'UInt128': types.UInt128,
    'UInt64': types.UInt64,
    'UInt32': types.UInt32,
    'UInt16': types.UInt16,
    'UInt8': types.UInt8,
    'Date': DATE,
    'DateTime': DATETIME,
    'DateTime64': DATETIME,
    'Float64': FLOAT,
    'Float32': FLOAT,
    'Decimal': types.Decimal,
    'String': types.String,
    'UUID': types.UUID,
    'IPv4': types.IPv4,
    'IPv6': types.IPv6,
    'FixedString': types.String,
    'Enum8': types.Enum8,
    'Enum16': types.Enum16,
    '_array': types.Array,
    '_nullable': types.Nullable,
    '_lowcardinality': types.LowCardinality,
    '_tuple': types.Tuple,
    '_map': types.Map,
}


class ByteHouseIdentifierPreparer(compiler.IdentifierPreparer):

    reserved_words = compiler.IdentifierPreparer.reserved_words | set((
        'index',  # reserved in the 'create table' syntax, at least.
    ))
    # Alternatively, use `_requires_quotes = lambda self, value: True`

    def _escape_identifier(self, value):
        value = value.replace(self.escape_quote, self.escape_to_quote)
        return value.replace('%', '%%')


class ByteHouseExecutionContextBase(default.DefaultExecutionContext):
    @sa_util.memoized_property
    def should_autocommit(self):
        return False  # No DML supported, never autocommit


class ByteHouseDialect(default.DefaultDialect):
    name = 'bytehouse'
    supports_cast = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_native_decimal = True
    supports_native_boolean = False
    non_native_boolean_check_constraint = False
    supports_alter = True
    supports_sequences = False
    supports_native_enum = True  # Do not render check constraints on enums.
    supports_multivalues_insert = True
    supports_statement_cache = True

    supports_comments = True
    inline_comments = True

    # Dialect related-features
    supports_delete = True
    supports_update = True
    supports_engine_reflection = True
    supports_table_comment_reflection = True

    engine_reflection = True  # Disables engine reflection from URL.

    max_identifier_length = 127
    default_paramstyle = 'pyformat'
    colspecs = colspecs
    ischema_names = ischema_names
    convert_unicode = True
    returns_unicode_strings = True
    description_encoding = None
    postfetch_lastrowid = False
    forced_server_version_string = None

    preparer = ByteHouseIdentifierPreparer
    type_compiler = ByteHouseTypeCompiler
    statement_compiler = ByteHouseSQLCompiler
    ddl_compiler = ByteHouseDDLCompiler

    construct_arguments = [
        (schema.Table, {
            'data': [],
            'cluster': None,
        }),
        (schema.Column, {
            'codec': None,
            'materialized': None,
            'alias': None,
            'after': None,
        }),
    ]

    inspector = ByteHouseInspector

    def initialize(self, connection):
        super(ByteHouseDialect, self).initialize(connection)

        version = self.server_version_info

        self.supports_delete = version >= (1, 1, 54388)
        self.supports_update = version >= (18, 12, 14)
        self.supports_engine_reflection = version >= (18, 16)
        self.supports_table_comment_reflection = version >= (21, 6)

    def _execute(self, connection, sql, scalar=False, **kwargs):
        raise NotImplementedError

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        database = []
        if schema is None:
            query = 'SHOW DATABASES'
            for db in self._execute(connection, query):
                database.append(db[0])
        else:
            database.append(schema)

        view = []
        for db in database:
            query = 'SHOW TABLES FROM {}'.format(db)
            for tb in self._execute(connection, query):
                if tb[8] == 'VIEW':
                    view.append(tb[0])
        return view

    def get_bytehouse_table_names(self, connection, schema=None):
        database = []
        if schema is None:
            query = 'SHOW DATABASES'
            for db in self._execute(connection, query):
                database.append(db[0])
        else:
            database.append(schema)

        table = []
        for db in database:
            query = 'SHOW TABLES FROM {}'.format(db)
            for tb in self._execute(connection, query):
                table.append(tb[0])
        return table

    def has_table(self, connection, table_name, schema=None):
        return table_name in self.get_bytehouse_table_names(connection, schema)

    def _quote_table_name(self, table_name):
        # Use case: `describe table (select ...)`, over a TextClause.
        if isinstance(table_name, elements.TextClause):
            return str(table_name)
        return self.identifier_preparer.quote_identifier(table_name)

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        quote = self._quote_table_name
        if schema:
            qualified_name = quote(schema) + '.' + quote(table_name)
        else:
            qualified_name = quote(table_name)
        query = 'DESCRIBE TABLE {}'.format(qualified_name)
        rows = self._execute(connection, query)

        return [
            self._get_column_info(
                r.Name, r.Type, r.DefaultType, r.DefaultExpression,
                getattr(r, 'Comment', None)
            ) for r in rows
        ]

    def _get_column_info(self, name, format_type, default_type,
                         default_expression, comment):
        col_type = self._get_column_type(name, format_type)
        col_default = self._get_column_default(default_type,
                                               default_expression)
        result = {
            'name': name,
            'type': col_type,
            'nullable': format_type.startswith('Nullable('),
            'default': col_default,
            'comment': comment or None
        }
        return result

    def _get_column_default(self, default_type, default_expression):
        if default_type == 'DEFAULT':
            return default_expression
        return None

    def _get_column_type(self, name, spec):
        if spec.startswith('Array'):
            inner = spec[6:-1]
            coltype = self.ischema_names['_array']
            return coltype(self._get_column_type(name, inner))

        elif spec.startswith('FixedString'):
            length = int(spec[12:-1])
            return self.ischema_names['FixedString'](length)

        elif spec.startswith('Nullable'):
            inner = spec[9:-1]
            coltype = self.ischema_names['_nullable']
            return coltype(self._get_column_type(name, inner))

        elif spec.startswith('LowCardinality'):
            inner = spec[15:-1]
            coltype = self.ischema_names['_lowcardinality']
            return coltype(self._get_column_type(name, inner))

        elif spec.startswith('Tuple'):
            inner = spec[6:-1]
            coltype = self.ischema_names['_tuple']
            inner_types = [
                self._get_column_type(name, t.strip())
                for t in inner.split(',')
            ]
            return coltype(*inner_types)

        elif spec.startswith('Map'):
            inner = spec[4:-1]
            coltype = self.ischema_names['_map']
            inner_types = [
                self._get_column_type(name, t.strip())
                for t in inner.split(',')
            ]
            return coltype(*inner_types)

        elif spec.startswith('Enum'):
            pos = spec.find('(')
            type = spec[:pos]
            coltype = self.ischema_names[type]

            options = dict()
            if pos >= 0:
                options = self._parse_options(
                    spec[pos + 1: spec.rfind(')')]
                )
            if not options:
                return sqltypes.NullType

            type_enum = enum.Enum('%s_enum' % name, options)
            return lambda: coltype(type_enum)

        elif spec.lower().startswith('decimal'):
            coltype = self.ischema_names['Decimal']
            return coltype(*self._parse_decimal_params(spec))
        else:
            try:
                return self.ischema_names[spec]
            except KeyError:
                warn("Did not recognize type '%s' of column '%s'" %
                     (spec, name))
                return sqltypes.NullType

    @staticmethod
    def _parse_decimal_params(spec):
        ints = spec.split('(')[-1].split(')')[0]  # get all data in brackets
        precision, scale = ints.split(',')
        return int(precision.strip()), int(scale.strip())

    @staticmethod
    def _parse_options(option_string):
        options = dict()
        after_name = False
        escaped = False
        quote_character = None
        name = ''
        value = ''

        for ch in option_string:
            if escaped:
                name += ch
                escaped = False  # Accepting escaped character

            elif after_name:
                if ch in (' ', '='):
                    pass
                elif ch == ',':
                    options[name] = int(value)
                    after_name = False
                    name = ''
                    value = ''  # Reset before collecting new option
                else:
                    value += ch

            elif quote_character:
                if ch == '\\':
                    escaped = True
                elif ch == quote_character:
                    quote_character = None
                    after_name = True  # Start collecting option value
                else:
                    name += ch

            else:
                if ch == "'":
                    quote_character = ch

        if after_name:
            options.setdefault(name, int(value))  # Word after last comma

        return options

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        rows = self._execute(connection, 'SHOW DATABASES')
        return [row[0] for row in rows]

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # No support for foreign keys.
        return []

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # No support for primary keys.
        return []

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        # No support for indexes.
        return []

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        # query = text(
        #     "SELECT name FROM system.tables "
        #     "WHERE engine NOT LIKE '%View' "
        #     "AND name NOT LIKE '.inner%' "
        #     "AND database = :database"
        # )
        #
        # database = schema or connection.engine.url.database
        # rows = self._execute(connection, query, database=database)
        # return [row.name for row in rows]

        # TODO: Parse arguments & filer array based on that
        return self.get_bytehouse_table_names(connection, schema)

    @reflection.cache
    def get_engine(self, connection, table_name, schema=None, **kw):
        columns = [
            'name', 'engine_full', 'engine', 'partition_key', 'sorting_key',
            'primary_key', 'sampling_key'
        ]

        database = schema if schema else connection.engine.url.database

        query = text(
            'SELECT {} FROM system.tables '
            'WHERE database = :database AND name = :name'
            .format(', '.join(columns))
        )

        rows = self._execute(
            connection, query, database=database, name=table_name
        )

        row = next(rows, None)

        if row:
            return {x: getattr(row, x, None) for x in columns}

    @reflection.cache
    def get_table_comment(self, connection, table_name, schema=None, **kw):
        if not self.supports_table_comment_reflection:
            raise NotImplementedError()

        database = schema if schema else connection.engine.url.database

        query = text(
            'SELECT comment FROM system.tables '
            'WHERE database = :database AND name = :name'
        )
        comment = self._execute(
            connection, query, database=database, name=table_name, scalar=True
        )
        return {'text': comment or None}

    def do_rollback(self, dbapi_connection):
        # No support for transactions.
        pass

    def do_executemany(self, cursor, statement, parameters, context=None):
        # render single insert inplace
        if (
            context
            and context.isinsert
            and context.compiled.insert_single_values_expr
            and not len(context.compiled_parameters[0])
        ):
            parameters = None

        cursor.executemany(statement, parameters, context=context)

    def do_execute(self, cursor, statement, parameters, context=None):
        cursor.execute(statement, parameters, context=context)

    def _check_unicode_returns(self, connection, additional_tests=None):
        return True

    def _check_unicode_description(self, connection):
        return True

    def _get_server_version_info(self, connection):
        return 1, 1, 54388
        # version = self.forced_server_version_string
        #
        # if version is None:
        #     version = self._execute(
        #         connection, 'select version()', scalar=True
        #     )
        #
        # # The first three are numeric, but the last is an alphanumeric build.
        # return tuple(int(p) if p.isdigit() else p for p in version.split('.'))

    def _get_default_schema_name(self, connection):
        return "default"
        # return self._execute(
        #     connection, 'select currentDatabase()', scalar=True
        # )

    def connect(self, *cargs, **cparams):
        self.forced_server_version_string = cparams.pop(
            'server_version', self.forced_server_version_string)
        return super(ByteHouseDialect, self).connect(*cargs, **cparams)


bytehouse_dialect = ByteHouseDialect()
