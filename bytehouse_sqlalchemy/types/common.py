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

from sqlalchemy.sql.type_api import to_instance
from sqlalchemy import types


class ByteHouseTypeEngine(types.TypeEngine):
    def compile(self, dialect=None):
        from bytehouse_sqlalchemy.drivers.base import bytehouse_dialect

        return super(ByteHouseTypeEngine, self).compile(
            dialect=bytehouse_dialect
        )


class String(types.String, ByteHouseTypeEngine):
    pass


class Int(types.Integer, ByteHouseTypeEngine):
    pass


class Float(types.Float, ByteHouseTypeEngine):
    pass


class Array(ByteHouseTypeEngine):
    __visit_name__ = 'array'

    def __init__(self, item_type):
        self.item_type = item_type
        self.item_type_impl = to_instance(item_type)
        super(Array, self).__init__()

    def literal_processor(self, dialect):
        item_processor = self.item_type_impl.literal_processor(dialect)

        def process(value):
            processed_value = []
            for x in value:
                if item_processor:
                    x = item_processor(x)
                processed_value.append(x)
            return '[' + ', '.join(processed_value) + ']'
        return process


class Nullable(ByteHouseTypeEngine):
    __visit_name__ = 'nullable'

    def __init__(self, nested_type):
        self.nested_type = nested_type
        super(Nullable, self).__init__()


class UUID(String):
    __visit_name__ = 'uuid'


class LowCardinality(ByteHouseTypeEngine):
    __visit_name__ = 'lowcardinality'

    def __init__(self, nested_type):
        self.nested_type = nested_type
        super(LowCardinality, self).__init__()


class Int8(Int):
    __visit_name__ = 'int8'


class UInt8(Int):
    __visit_name__ = 'uint8'


class Int16(Int):
    __visit_name__ = 'int16'


class UInt16(Int):
    __visit_name__ = 'uint16'


class Int32(Int):
    __visit_name__ = 'int32'


class UInt32(Int):
    __visit_name__ = 'uint32'


class Int64(Int):
    __visit_name__ = 'int64'


class UInt64(Int):
    __visit_name__ = 'uint64'


class Int128(Int):
    __visit_name__ = 'int128'


class UInt128(Int):
    __visit_name__ = 'uint128'


class Int256(Int):
    __visit_name__ = 'int256'


class UInt256(Int):
    __visit_name__ = 'uint256'


class Float32(Float):
    __visit_name__ = 'float32'


class Float64(Float):
    __visit_name__ = 'float64'


class Date(types.Date, ByteHouseTypeEngine):
    __visit_name__ = 'date'


class DateTime(types.Date, ByteHouseTypeEngine):
    __visit_name__ = 'datetime'


class DateTime64(DateTime, ByteHouseTypeEngine):
    __visit_name__ = 'datetime64'

    def __init__(self, precision=3, timezone=None):
        self.precision = precision
        self.timezone = timezone
        super(DateTime64, self).__init__()


class Enum(types.Enum, ByteHouseTypeEngine):
    __visit_name__ = 'enum'

    def __init__(self, *enums, **kw):
        if not enums:
            enums = kw.get('_enums', ())  # passed as keyword

        super(Enum, self).__init__(*enums, **kw)


class Enum8(Enum):
    __visit_name__ = 'enum8'


class Enum16(Enum):
    __visit_name__ = 'enum16'


class Decimal(types.Numeric, ByteHouseTypeEngine):
    __visit_name__ = 'numeric'


class Tuple(ByteHouseTypeEngine):
    __visit_name__ = 'tuple'

    def __init__(self, *nested_types):
        self.nested_types = nested_types
        super(Tuple, self).__init__()


class Map(ByteHouseTypeEngine):
    __visit_name__ = 'map'

    def __init__(self, key_type, value_type):
        self.key_type = key_type
        self.value_type = value_type
        super(Map, self).__init__()
