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

from sqlalchemy.engine import reflection

from bytehouse_sqlalchemy import Table, engines


class ByteHouseInspector(reflection.Inspector):
    def reflect_table(self, table, *args, **kwargs):
        # This check is necessary to support direct instantiation of
        # `bytehouse_sqlalchemy.Table` and then reflection of it.
        if not isinstance(table, Table):
            table.metadata.remove(table)
            ch_table = Table._make_from_standard(
                table, _extend_on=kwargs.get('_extend_on')
            )
        else:
            ch_table = table

        super(ByteHouseInspector, self).reflect_table(
            ch_table, *args, **kwargs
        )

        with self._operation_context() as conn:
            schema = conn.schema_for_object(ch_table)

            self._reflect_engine(ch_table.name, schema, ch_table)

    def _reflect_engine(self, table_name, schema, table):
        should_reflect = (
            self.dialect.supports_engine_reflection and
            self.dialect.engine_reflection
        )
        if not should_reflect:
            return

        engine_cls_by_name = {e.__name__: e for e in engines.__all__}

        e = self.get_engine(table_name, schema=table.schema)
        if not e:
            raise ValueError("Cannot find engine for table '%s'" % table_name)

        engine_cls = engine_cls_by_name.get(e['engine'])
        if engine_cls is not None:
            engine = engine_cls.reflect(table, **e)
            engine._set_parent(table)
        else:
            table.engine = None

    def get_engine(self, table_name, schema=None, **kw):
        with self._operation_context() as conn:
            return self.dialect.get_engine(
                conn, table_name, schema=schema, info_cache=self.info_cache,
                **kw
            )
