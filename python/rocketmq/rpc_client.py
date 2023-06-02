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
import time
from datetime import timedelta

import certifi
from grpc import aio, ssl_channel_credentials
from protocol import service_pb2
from rocketmq import logger
from rocketmq.protocol import service_pb2_grpc


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
        self, request: service_pb2.QueryRouteRequest, timeout_seconds: int
    ):
        return await self.__stub.QueryRoute(request, timeout=timeout_seconds)

    async def heartbeat(
        self, request: service_pb2.HeartbeatRequest, timeout_seconds: int
    ):
        return await self.__stub.Heartbeat(request, timeout=timeout_seconds)

    async def send_message(
        self, request: service_pb2.SendMessageRequest, timeout_seconds: int
    ):
        return await self.__stub.SendMessage(request, timeout=timeout_seconds)

    async def query_assignment(
        self, request: service_pb2.QueryAssignmentRequest, timeout_seconds: int
    ):
        return await self.__stub.QueryAssignment(request, timeout=timeout_seconds)

    async def ack_message(
        self, request: service_pb2.AckMessageRequest, timeout_seconds: int
    ):
        return await self.__stub.AckMessage(request, timeout=timeout_seconds)

    async def forward_message_to_dead_letter_queue(
        self,
        request: service_pb2.ForwardMessageToDeadLetterQueueRequest,
        timeout_seconds: int,
    ):
        return await self.__stub.ForwardMessageToDeadLetterQueue(
            request, timeout=timeout_seconds
        )

    async def end_transaction(
        self, request: service_pb2.EndTransactionRequest, timeout_seconds: int
    ):
        return await self.__stub.EndTransaction(request, timeout=timeout_seconds)

    async def notify_client_termination(
        self, request: service_pb2.NotifyClientTerminationRequest, timeout_seconds: int
    ):
        return await self.__stub.NotifyClientTermination(
            request, timeout=timeout_seconds
        )

    async def change_invisible_duration(
        self, request: service_pb2.ChangeInvisibleDurationRequest, timeout_seconds: int
    ):
        return await self.__stub.ChangeInvisibleDuration(
            request, timeout=timeout_seconds
        )


async def test():
    client = RpcClient("rmq-cn-72u353icd01.cn-hangzhou.rmq.aliyuncs.com:8080")
    request = service_pb2.QueryRouteRequest()
    response = await client.query_route(request, 3)
    logger.info(response)


if __name__ == "__main__":
    asyncio.run(test())
