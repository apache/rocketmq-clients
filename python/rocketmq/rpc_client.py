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

import asyncio
import operator
import socket
import time
from datetime import timedelta
from enum import Enum
from functools import reduce

import certifi
from grpc import aio, ssl_channel_credentials
from rocketmq.log import logger
from rocketmq.protocol import service_pb2, service_pb2_grpc
from rocketmq.protocol.definition_pb2 import Address as ProtoAddress
from rocketmq.protocol.definition_pb2 import \
    AddressScheme as ProtoAddressScheme
from rocketmq.protocol.definition_pb2 import Endpoints as ProtoEndpoints


class AddressScheme(Enum):
    Unspecified = 0
    Ipv4 = 1
    Ipv6 = 2
    DomainName = 3

    @staticmethod
    def to_protobuf(scheme):
        if scheme == AddressScheme.Ipv4:
            return ProtoAddressScheme.IPV4
        elif scheme == AddressScheme.Ipv6:
            return ProtoAddressScheme.IPV6
        elif scheme == AddressScheme.DomainName:
            return ProtoAddressScheme.DOMAIN_NAME
        else:  # Unspecified or other cases
            return ProtoAddressScheme.ADDRESS_SCHEME_UNSPECIFIED


class Address:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def to_protobuf(self):
        proto_address = ProtoAddress()
        proto_address.host = self.host
        proto_address.port = self.port
        return proto_address


class Endpoints:
    HttpPrefix = "http://"
    HttpsPrefix = "https://"
    DefaultPort = 80
    EndpointSeparator = ":"

    def __init__(self, endpoints):
        self.Addresses = []

        self.scheme = AddressScheme.Unspecified
        self._hash = None

        if type(endpoints) == str:
            if endpoints.startswith(self.HttpPrefix):
                endpoints = endpoints[len(self.HttpPrefix):]
            if endpoints.startswith(self.HttpsPrefix):
                endpoints = endpoints[len(self.HttpsPrefix):]

            index = endpoints.find(self.EndpointSeparator)
            port = int(endpoints[index + 1:]) if index > 0 else 80
            host = endpoints[:index] if index > 0 else endpoints
            address = Address(host, port)
            self.Addresses.append(address)
            try:
                socket.inet_pton(socket.AF_INET, host)
                self.scheme = AddressScheme.IPv4
            except socket.error:
                try:
                    socket.inet_pton(socket.AF_INET6, host)
                    self.scheme = AddressScheme.IPv6
                except socket.error:
                    self.scheme = AddressScheme.DomainName
            self.Addresses.append(address)

            # Assuming AddressListEqualityComparer exists
            self._hash = 17
            self._hash = (self._hash * 31) + reduce(
                operator.xor, (hash(address) for address in self.Addresses)
            )
            self._hash = (self._hash * 31) + hash(self.scheme)
        else:
            self.Addresses = [
                Address(addr.host, addr.port) for addr in endpoints.addresses
            ]
            if not self.Addresses:
                raise Exception("No available address")

            if endpoints.scheme == "Ipv4":
                self.scheme = AddressScheme.Ipv4
            elif endpoints.scheme == "Ipv6":
                self.scheme = AddressScheme.Ipv6
            else:
                self.scheme = AddressScheme.DomainName
                if len(self.Addresses) > 1:
                    raise Exception(
                        "Multiple addresses are\
                                    not allowed in domain scheme"
                    )

            self._hash = self._calculate_hash()

    def _calculate_hash(self):
        hash_value = 17
        for address in self.Addresses:
            hash_value = (hash_value * 31) + hash(address)
        hash_value = (hash_value * 31) + hash(self.scheme)
        return hash_value

    def __str__(self):
        for address in self.Addresses:
            return str(address.host) + str(address.port)

    def grpc_target(self, sslEnabled):
        for address in self.Addresses:
            return address.host + ":" + str(address.port)
        raise ValueError("No available address")

    def __eq__(self, other):
        if other is None:
            return False
        if self is other:
            return True
        res = self.Addresses == other.Addresses and self.Scheme == other.Scheme
        return res

    def __hash__(self):
        return self._hash

    def to_protobuf(self):
        proto_endpoints = ProtoEndpoints()
        proto_endpoints.scheme = self.scheme.to_protobuf(self.scheme)
        proto_endpoints.addresses.extend([i.to_protobuf() for i in self.Addresses])
        return proto_endpoints


class RpcClient:
    channel_options = [
        ("grpc.max_send_message_length", -1),
        ("grpc.max_receive_message_length", -1),
        ("grpc.connect_timeout_ms", 3000),
    ]

    def __init__(self, endpoints: str, ssl_enabled: bool = True):
        self.__endpoints = endpoints
        self.__cert = certifi.contents().encode(encoding="utf-8")
        if ssl_enabled:
            self.__channel = aio.secure_channel(
                endpoints,
                ssl_channel_credentials(root_certificates=self.__cert),
                options=RpcClient.channel_options,
            )
        else:
            self.__channel = aio.insecure_channel(
                endpoints, options=RpcClient.channel_options
            )
        self.__stub = service_pb2_grpc.MessagingServiceStub(self.__channel)
        self.activity_nano_time = time.monotonic_ns()

    def idle_duration(self):
        return timedelta(
            microseconds=(time.monotonic_ns() - self.activity_nano_time) / 1000
        )

    async def query_route(
        self, request: service_pb2.QueryRouteRequest, metadata, timeout_seconds: int
    ):
        # metadata = [('x-mq-client-id', 'value1')]
        return await self.__stub.QueryRoute(
            request, timeout=timeout_seconds, metadata=metadata
        )

    async def heartbeat(
        self, request: service_pb2.HeartbeatRequest, metadata, timeout_seconds: int
    ):
        return await self.__stub.Heartbeat(
            request, metadata=metadata, timeout=timeout_seconds
        )

    async def send_message(
        self, request: service_pb2.SendMessageRequest, metadata, timeout_seconds: int
    ):
        return await self.__stub.SendMessage(
            request, metadata=metadata, timeout=timeout_seconds
        )

    async def receive_message(
        self, request: service_pb2.ReceiveMessageRequest, metadata, timeout_seconds: int
    ):
        results = self.__stub.ReceiveMessage(
            request, metadata=metadata, timeout=timeout_seconds
        )
        response = []
        try:
            async for result in results:
                if result.HasField("message"):
                    response.append(result.message)
        except Exception as e:
            logger.info("An error occurred: %s", e)
            # Handle error as appropriate for your use case
        return response

    async def query_assignment(
        self,
        request: service_pb2.QueryAssignmentRequest,
        metadata,
        timeout_seconds: int,
    ):
        return await self.__stub.QueryAssignment(
            request, metadata=metadata, timeout=timeout_seconds
        )

    async def ack_message(
        self, request: service_pb2.AckMessageRequest, metadata, timeout_seconds: int
    ):
        return await self.__stub.AckMessage(
            request, metadata=metadata, timeout=timeout_seconds
        )

    async def forward_message_to_dead_letter_queue(
        self,
        request: service_pb2.ForwardMessageToDeadLetterQueueRequest,
        metadata,
        timeout_seconds: int,
    ):
        return await self.__stub.ForwardMessageToDeadLetterQueue(
            request, metadata=metadata, timeout=timeout_seconds
        )

    async def end_transaction(
        self, request: service_pb2.EndTransactionRequest, metadata, timeout_seconds: int
    ):
        return await self.__stub.EndTransaction(
            request, metadata=metadata, timeout=timeout_seconds
        )

    async def notify_client_termination(
        self,
        request: service_pb2.NotifyClientTerminationRequest,
        metadata,
        timeout_seconds: int,
    ):
        return await self.__stub.NotifyClientTermination(
            request, metadata=metadata, timeout=timeout_seconds
        )

    async def change_invisible_duration(
        self,
        request: service_pb2.ChangeInvisibleDurationRequest,
        metadata,
        timeout_seconds: int,
    ):
        return await self.__stub.ChangeInvisibleDuration(
            request, metadata=metadata, timeout=timeout_seconds
        )

    async def send_requests(self, requests, stream):
        for request in requests:
            await stream.send_message(request)

    def telemetry(self, metadata, timeout_seconds: int):
        stream = self.__stub.Telemetry(metadata=metadata, timeout=timeout_seconds)
        return stream


async def test():
    client = RpcClient("rmq-cn-jaj390gga04.cn-hangzhou.rmq.aliyuncs.com:8081")
    request = service_pb2.SendMessageRequest()
    response = await client.send_message(request, 3)
    logger.info(response)


if __name__ == "__main__":
    asyncio.run(test())
