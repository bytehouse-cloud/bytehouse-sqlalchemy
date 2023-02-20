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

import configparser

from sqlalchemy.dialects import registry

from tests import log

registry.register(
    "bytehouse", "bytehouse_sqlalchemy.drivers.native.base", "dialect"
)

file_config = configparser.ConfigParser()
file_config.read(['setup.cfg'])

log.configure(file_config.get('log', 'level'))

region = file_config.get('db', 'region')
database = file_config.get('db', 'database')
account = file_config.get('db', 'account')
user = file_config.get('db', 'user')
password = file_config.get('db', 'password')

uri_template = '{schema}:///?region={region}&account={account}&user={user}&password={password}&database={database}'

native_uri = uri_template.format(
    schema='bytehouse', user=user, password=password, region=region,
    account=account, database=database)

system_native_uri = uri_template.format(
    schema='bytehouse', user=user, password=password, region=region,
    account=account, database=database)
