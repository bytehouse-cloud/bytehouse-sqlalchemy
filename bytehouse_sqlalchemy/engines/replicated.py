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

# from .cnchmergetree import (
#     CnchMergeTree, AggregatingCnchMergeTree, CollapsingMergeTree,
#     VersionedCollapsingMergeTree, ReplacingMergeTree, SummingMergeTree
# )
# from .util import parse_columns
#
#
# class ReplicatedEngineMixin(object):
#     def __init__(self, table_path, replica_name):
#         self.table_path = table_path
#         self.replica_name = replica_name
#
#     def get_parameters(self):
#         return [
#             "'{}'".format(self.table_path),
#             "'{}'".format(self.replica_name)
#         ]
#
#     @classmethod
#     def _reflect_replicated(cls, engine_full):
#         engine = parse_columns(engine_full, delimeter=' ')[0]
#         columns = engine[len(cls.__name__):][1:-1]
#         return parse_columns(columns)
#
#
# class ReplicatedMergeTree(ReplicatedEngineMixin, CnchMergeTree):
#     def __init__(self, table_path, replica_name,
#                  *args, **kwargs):
#         ReplicatedEngineMixin.__init__(self, table_path, replica_name)
#         CnchMergeTree.__init__(self, *args, **kwargs)
#
#     def get_parameters(self):
#         return self.extend_parameters(
#             ReplicatedEngineMixin.get_parameters(self),
#             CnchMergeTree.get_parameters(self)
#         )
#
#     @classmethod
#     def reflect(cls, table, engine_full, **kwargs):
#         args = cls._reflect_replicated(engine_full)
#         table_path, replica_name = args[:2]
#
#         return cls(
#             table_path.strip("'"), replica_name.strip("'"),
#             *args[2:],
#             **cls._reflect_merge_tree(table, **kwargs)
#         )
#
#
# class ReplicatedAggregatingMergeTree(ReplicatedEngineMixin,
#                                      AggregatingCnchMergeTree):
#     def __init__(self, table_path, replica_name,
#                  *args, **kwargs):
#         ReplicatedEngineMixin.__init__(self, table_path, replica_name)
#         AggregatingCnchMergeTree.__init__(self, *args, **kwargs)
#
#     def get_parameters(self):
#         return self.extend_parameters(
#             ReplicatedEngineMixin.get_parameters(self),
#             AggregatingCnchMergeTree.get_parameters(self)
#         )
#
#     @classmethod
#     def reflect(cls, table, engine_full, **kwargs):
#         args = cls._reflect_replicated(engine_full)
#         table_path, replica_name = args[:2]
#
#         return cls(
#             table_path.strip("'"), replica_name.strip("'"),
#             *args[2:],
#             **cls._reflect_merge_tree(table, **kwargs)
#         )
#
#
# class ReplicatedCollapsingMergeTree(ReplicatedEngineMixin,
#                                     CollapsingMergeTree):
#     def __init__(self, table_path, replica_name,
#                  *args, **kwargs):
#         ReplicatedEngineMixin.__init__(self, table_path, replica_name)
#         CollapsingMergeTree.__init__(self, *args, **kwargs)
#
#     def get_parameters(self):
#         return self.extend_parameters(
#             ReplicatedEngineMixin.get_parameters(self),
#             CollapsingMergeTree.get_parameters(self)
#         )
#
#     @classmethod
#     def reflect(cls, table, engine_full, **kwargs):
#         args = cls._reflect_replicated(engine_full)
#         table_path, replica_name = args[:2]
#
#         return cls(
#             table_path.strip("'"), replica_name.strip("'"),
#             *args[2:],
#             **cls._reflect_merge_tree(table, **kwargs)
#         )
#
#
# class ReplicatedVersionedCollapsingMergeTree(ReplicatedEngineMixin,
#                                              VersionedCollapsingMergeTree):
#     def __init__(self, table_path, replica_name,
#                  *args, **kwargs):
#         ReplicatedEngineMixin.__init__(self, table_path, replica_name)
#         VersionedCollapsingMergeTree.__init__(self, *args, **kwargs)
#
#     def get_parameters(self):
#         return self.extend_parameters(
#             ReplicatedEngineMixin.get_parameters(self),
#             VersionedCollapsingMergeTree.get_parameters(self)
#         )
#
#     @classmethod
#     def reflect(cls, table, engine_full, **kwargs):
#         args = cls._reflect_replicated(engine_full)
#         table_path, replica_name = args[:2]
#
#         return cls(
#             table_path.strip("'"), replica_name.strip("'"),
#             *args[2:],
#             **cls._reflect_merge_tree(table, **kwargs)
#         )
#
#
# class ReplicatedReplacingMergeTree(ReplicatedEngineMixin, ReplacingMergeTree):
#     def __init__(self, table_path, replica_name,
#                  *args, **kwargs):
#         ReplicatedEngineMixin.__init__(self, table_path, replica_name)
#         ReplacingMergeTree.__init__(self, *args, **kwargs)
#
#     def get_parameters(self):
#         return self.extend_parameters(
#             ReplicatedEngineMixin.get_parameters(self),
#             ReplacingMergeTree.get_parameters(self)
#         )
#
#     @classmethod
#     def reflect(cls, table, engine_full, **kwargs):
#         args = cls._reflect_replicated(engine_full)
#         table_path, replica_name = args[:2]
#         version = None
#         if len(args) > 2:
#             version = args[2]
#
#         return cls(
#             table_path.strip("'"), replica_name.strip("'"),
#             version=version,
#             **cls._reflect_merge_tree(table, **kwargs)
#         )
#
#
# class ReplicatedSummingMergeTree(ReplicatedEngineMixin, SummingMergeTree):
#     def __init__(self, table_path, replica_name,
#                  *args, **kwargs):
#         ReplicatedEngineMixin.__init__(self, table_path, replica_name)
#         SummingMergeTree.__init__(self, *args, **kwargs)
#
#     def get_parameters(self):
#         return self.extend_parameters(
#             ReplicatedEngineMixin.get_parameters(self),
#             SummingMergeTree.get_parameters(self)
#         )
#
#     @classmethod
#     def reflect(cls, table, engine_full, **kwargs):
#         args = cls._reflect_replicated(engine_full)
#         table_path, replica_name = args[:2]
#         columns = None
#         if len(args) > 2:
#             columns = tuple(parse_columns(args[2].strip('()')))
#
#         return cls(
#             table_path.strip("'"), replica_name.strip("'"),
#             columns=columns,
#             **cls._reflect_merge_tree(table, **kwargs)
#         )
