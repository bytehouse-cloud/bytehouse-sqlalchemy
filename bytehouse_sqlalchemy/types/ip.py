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

from ipaddress import IPv4Network, IPv6Network

from sqlalchemy import or_, and_, types, func
from sqlalchemy.sql.type_api import UserDefinedType


class BaseIPComparator(UserDefinedType.Comparator):
    network_class = None

    def _wrap_to_ip(self, x):
        raise NotImplementedError()

    def _split_other(self, other):
        """
        Split values between addresses and networks
        This allows to generate complex filters with both addresses
        and networks in the same IN
        ie in_('10.0.0.0/24', '192.168.0.1')
        """
        addresses = []
        networks = []
        for sub in other:
            sub = self.network_class(sub)
            if sub.prefixlen == sub.max_prefixlen:
                # this is an address
                addresses.append(sub.network_address)
            else:
                networks.append(sub)
        return addresses, networks

    def in_(self, other):
        if isinstance(other, (list, tuple)):
            addresses, networks = self._split_other(other)
            addresses_clause = super(BaseIPComparator, self).in_(
                self._wrap_to_ip(x) for x in addresses
            ) if addresses else None
            networks_clause = or_(*[
                and_(
                    self >= self._wrap_to_ip(net[0]),
                    self <= self._wrap_to_ip(net[-1])
                )
                for net in networks
            ]) if networks else None
            if addresses_clause is not None and networks_clause is not None:
                return or_(addresses_clause, networks_clause)
            elif addresses_clause is not None and networks_clause is None:
                return addresses_clause
            elif networks_clause is not None and addresses_clause is None:
                return networks_clause
            else:
                # other is an empty array
                return super(BaseIPComparator, self).in_(other)

        if not isinstance(other, self.network_class):
            other = self.network_class(other)

        return and_(
            self >= self._wrap_to_ip(other[0]),
            self <= self._wrap_to_ip(other[-1])
        )

    def not_in(self, other):
        if isinstance(other, (list, tuple)):
            addresses, networks = self._split_other(other)
            addresses_clause = super(BaseIPComparator, self).notin_(
                self._wrap_to_ip(x) for x in addresses
            ) if addresses else None
            networks_clause = and_(*[
                or_(
                    self < self._wrap_to_ip(net[0]),
                    self > self._wrap_to_ip(net[-1])
                )
                for net in networks
            ]) if networks else None
            if addresses_clause is not None and networks_clause is not None:
                return and_(addresses_clause, networks_clause)
            elif addresses_clause is not None and networks_clause is None:
                return addresses_clause
            elif networks_clause is not None and addresses_clause is None:
                return networks_clause
            else:
                # other is an empty array
                return super(BaseIPComparator, self).notin_(other)

        if not isinstance(other, self.network_class):
            other = self.network_class(other)

        return or_(
            self < self._wrap_to_ip(other[0]),
            self > self._wrap_to_ip(other[-1])
        )


class IPv4(types.UserDefinedType):
    __visit_name__ = "ipv4"

    cache_ok = True

    def bind_processor(self, dialect):
        def process(value):
            return str(value)

        return process

    def literal_processor(self, dialect):
        bp = self.bind_processor(dialect)

        def process(value):
            return "'%s'" % bp(value)

        return process

    def bind_expression(self, bindvalue):
        if isinstance(bindvalue.value, (list, tuple)):
            bindvalue.value = ([func.toIPv4(x) for x in bindvalue.value])
            return bindvalue
        return func.toIPv4(bindvalue)

    class comparator_factory(BaseIPComparator):
        network_class = IPv4Network

        def _wrap_to_ip(self, x):
            return func.toIPv4(str(x))


class IPv6(types.UserDefinedType):
    __visit_name__ = "ipv6"

    cache_ok = True

    def bind_processor(self, dialect):
        def process(value):
            return str(value)

        return process

    def literal_processor(self, dialect):
        bp = self.bind_processor(dialect)

        def process(value):
            return "'%s'" % bp(value)

        return process

    def bind_expression(self, bindvalue):
        if isinstance(bindvalue.value, (list, tuple)):
            bindvalue.value = ([func.toIPv6(x) for x in bindvalue.value])
            return bindvalue
        return func.toIPv6(bindvalue)

    class comparator_factory(BaseIPComparator):
        network_class = IPv6Network

        def _wrap_to_ip(self, x):
            return func.toIPv6(str(x))
