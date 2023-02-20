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

def parse_columns(str_columns, delimeter=',', quote_symbol='`',
                  escape_symbol='\\'):
    if not str_columns:
        return []

    in_column = False
    quoted = False
    prev_symbol = None
    brackets_count = 0

    rv = []
    col = ''
    for i, x in enumerate(str_columns + delimeter):
        if x == delimeter and not quoted and brackets_count == 0:
            in_column = False
            rv.append(col)
            col = ''

        elif x == ' ' and not in_column:
            continue

        elif x == '(':
            brackets_count += 1
            col += x

        elif x == ')':
            brackets_count -= 1
            col += x

        else:
            if x == quote_symbol:
                if prev_symbol != escape_symbol:
                    if not quoted:
                        quoted = True
                        in_column = True
                    else:
                        quoted = False
                        in_column = False
                else:
                    col = col[:-1] + x

            else:
                if not in_column:
                    in_column = True

                col += x

        prev_symbol = x

    return rv
