# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import socket

from rocketmq.grpc_protocol import AddressScheme, Endpoints
from rocketmq.v5.client.connection import RpcEndpoints
from rocketmq.v5.log import logger


class Credentials:

    def __init__(self, ak="", sk=""):
        self.__ak = ak if ak is not None else ""
        self.__sk = sk if sk is not None else ""

    @property
    def ak(self):
        return self.__ak

    @property
    def sk(self):
        return self.__sk


class ClientConfiguration:

    def __init__(
        self, endpoints: str, credentials: Credentials, namespace="", request_timeout=3
    ):
        self.__rpc_endpoints = RpcEndpoints(
            ClientConfiguration.__parse_endpoints(endpoints)
        )
        self.__credentials = credentials
        self.__request_timeout = request_timeout  # seconds
        self.__namespace = namespace

    @staticmethod
    def __parse_endpoints(endpoints_str):
        if len(endpoints_str) == 0:
            return None
        else:
            try:
                endpoints = Endpoints()
                addresses = endpoints_str.split(";")
                endpoints.scheme = ClientConfiguration.__parse_endpoints_scheme_type(
                    ClientConfiguration.__parse_endpoints_prefix(
                        addresses[0].split(":")[0]
                    )
                )
                for address in addresses:
                    if len(address) == 0:
                        continue
                    ad = endpoints.addresses.add()
                    address = ClientConfiguration.__parse_endpoints_prefix(address)
                    ad.host = address.split(":")[0]
                    ad.port = int(address.split(":")[1])
                return endpoints
            except Exception as e:
                logger.error(
                    f"client configuration parse {endpoints_str} exception: {e}"
                )
                return None

    @staticmethod
    def __parse_endpoints_scheme_type(host):
        try:
            socket.inet_pton(socket.AF_INET, host)
            return AddressScheme.IPv4
        except socket.error:
            try:
                socket.inet_pton(socket.AF_INET6, host)
                return AddressScheme.IPv6
            except socket.error:
                return AddressScheme.DOMAIN_NAME

    @staticmethod
    def __parse_endpoints_prefix(endpoints_str):
        http_prefix = "http://"
        https_prefix = "https://"
        if endpoints_str.startswith(http_prefix):
            return endpoints_str[len(http_prefix):]
        elif endpoints_str.startswith(https_prefix):
            return endpoints_str[len(https_prefix):]
        return endpoints_str

    """ property """

    @property
    def rpc_endpoints(self) -> RpcEndpoints:
        return self.__rpc_endpoints

    @property
    def namespace(self):
        return self.__namespace

    @property
    def credentials(self):
        return self.__credentials

    @property
    def request_timeout(self):
        return self.__request_timeout
