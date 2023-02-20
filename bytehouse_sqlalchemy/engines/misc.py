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

#
# from .base import Engine
# from .util import parse_columns
#
#
# class Distributed(Engine):
#     def __init__(self, logs, default, hits, sharding_key=None):
#         self.logs = logs
#         self.default = default
#         self.hits = hits
#         self.sharding_key = sharding_key
#         super(Distributed, self).__init__()
#
#     def get_parameters(self):
#         params = [
#             self.logs,
#             self.default,
#             self.hits
#         ]
#
#         if self.sharding_key is not None:
#             params.append(self.sharding_key)
#
#         return params
#
#     @classmethod
#     def reflect(cls, table, engine_full, **kwargs):
#         engine = parse_columns(engine_full, delimeter=' ')[0]
#         columns = engine.split('(', 1)[1][:-1]
#         columns = parse_columns(columns)
#
#         return cls(*columns)
#
#
# class Buffer(Engine):
#     def __init__(self, database, table, num_layers=16,
#                  min_time=10, max_time=100, min_rows=10000, max_rows=1000000,
#                  min_bytes=10000000, max_bytes=100000000):
#         self.database = database
#         self.table_name = table
#         self.num_layers = num_layers
#         self.min_time = min_time
#         self.max_time = max_time
#         self.min_rows = min_rows
#         self.max_rows = max_rows
#         self.min_bytes = min_bytes
#         self.max_bytes = max_bytes
#         super(Buffer, self).__init__()
#
#     def get_parameters(self):
#         return [
#             self.database,
#             self.table_name,
#             self.num_layers,
#             self.min_time,
#             self.max_time,
#             self.min_rows,
#             self.max_rows,
#             self.min_bytes,
#             self.max_bytes
#         ]
#
#     @classmethod
#     def reflect(cls, table, engine_full, **kwargs):
#         engine = parse_columns(engine_full, delimeter=' ')[0]
#         params = parse_columns(engine[len(cls.__name__):].strip("()"))
#
#         database = params[0]
#         table_name = params[1]
#         num_layers = int(params[2])
#         min_time = int(params[3])
#         max_time = int(params[4])
#         min_rows = int(params[5])
#         max_rows = int(params[6])
#         min_bytes = int(params[7])
#         max_bytes = int(params[8])
#
#         return cls(
#             database, table_name, num_layers, min_time, max_time,
#             min_rows, max_rows, min_bytes, max_bytes
#         )
#
#
# class _NoParamsEngine(Engine):
#     @classmethod
#     def reflect(cls, table, engine_full, **kwargs):
#         return cls()
#
#
# class View(_NoParamsEngine):
#     pass
#
#
# class MaterializedView(_NoParamsEngine):
#     pass
#
#
# class TinyLog(_NoParamsEngine):
#     pass
#
#
# class Log(_NoParamsEngine):
#     pass
#
#
# class Memory(_NoParamsEngine):
#     pass
#
#
# class Null(_NoParamsEngine):
#     pass
#
#
# class CnchMergeTree(_NoParamsEngine):
#     pass
#
#
# class File(Engine):
#     supported_data_formats = {
#         'tabseparated': 'TabSeparated',
#         'tabseparatedwithnames': 'TabSeparatedWithNames',
#         'tabseparatedwithnamesandtypes': 'TabSeparatedWithNamesAndTypes',
#         'template': 'Template',
#         'csv': 'CSV',
#         'csvwithnames': 'CSVWithNames',
#         'customseparated': 'CustomSeparated',
#         'values': 'Values',
#         'jsoneachrow': 'JSONEachRow',
#         'tskv': 'TSKV',
#         'protobuf': 'Protobuf',
#         'parquet': 'Parquet',
#         'rowbinary': 'RowBinary',
#         'rowbinarywithnamesandtypes': 'RowBinaryWithNamesAndTypes',
#         'native': 'Native'
#     }
#
#     def __init__(self, data_format):
#         fmt = self.supported_data_formats.get(data_format.lower())
#         if not fmt:
#             raise ValueError(
#                 'Format {0} not supported for File engine'.format(
#                     data_format
#                 )
#             )
#         self.data_format = fmt
#         super(File, self).__init__()
#
#     def get_parameters(self):
#         return (self.data_format, )
#
#     @classmethod
#     def reflect(cls, table, engine_full, **kwargs):
#         fmt = engine_full.split('(', 1)[1][:-1].strip("'")
#         return cls(fmt)
