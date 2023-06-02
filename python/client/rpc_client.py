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

import time
from datetime import timedelta

import protocol.service_pb2_grpc as service
from grpc import aio, insecure_channel, ssl_channel_credentials


class RpcClient:
    CONNECT_TIMEOUT_MILLIS = 3 * 1000
    GRPC_MAX_MESSAGE_SIZE = 2 * 31 - 1

    def __init__(self, endpoints, ssl_enabled):
        channel_options = [
            ("grpc.max_send_message_length", -1),
            ("grpc.max_receive_message_length", -1),
            ("grpc.connect_timeout_ms", self.CONNECT_TIMEOUT_MILLIS),
        ]
        if ssl_enabled:
            ssl_credentials = ssl_channel_credentials()
            self.channel = aio.secure_channel(
                endpoints.getGrpcTarget(), ssl_credentials, options=channel_options
            )
        else:
            self.channel = insecure_channel(
                endpoints.getGrpcTarget(), options=channel_options
            )

        self.activity_nano_time = time.monotonic_ns()

    def __del__(self):
        self.channel.close()

    def idle_duration(self):
        return timedelta(
            microseconds=(time.monotonic_ns() - self.activity_nano_time) / 1000
        )

    async def query_route(self, request, timeout_seconds: int):
        self.activity_nano_time = time.monotonic_ns()
        stub = service.MessagingServiceStub(self.channel)
        return await stub.QueryRoute(request, timeout=timeout_seconds)

    async def heartbeat(self, request, timeout_seconds: int):
        self.activity_nano_time = time.monotonic_ns()
        stub = service.MessagingServiceStub(self.channel)
        return await stub.Heartbeat(request, timeout=timeout_seconds)

    async def send_message(self, request, timeout_seconds: int):
        self.activity_nano_time = time.monotonic_ns()
        stub = service.MessagingServiceStub(self.channel)
        return await stub.SendMessage(request, timeout=timeout_seconds)

    async def query_assignment(self, request, timeout_seconds: int):
        self.activity_nano_time = time.monotonic_ns()
        stub = service.MessagingServiceStub(self.channel)
        return await stub.QueryAssignment(request, timeout=timeout_seconds)

    # TODO: Not yet implemented
    async def receive_message(self, metadata, request, timeout_seconds: int):
        pass

    async def ack_message(self, request, timeout_seconds: int):
        self.activity_nano_time = time.monotonic_ns()
        stub = service.MessagingServiceStub(self.channel)
        return await stub.AckMessage(request, timeout=timeout_seconds)

    async def change_invisible_duration(self, request, timeout_seconds: int):
        self.activity_nano_time = time.monotonic_ns()
        stub = service.MessagingServiceStub(self.channel)
        return await stub.ChangeInvisibleDuration(request, timeout=timeout_seconds)

    async def forward_message_to_dead_letter_queue(self, request, timeout_seconds: int):
        self.activity_nano_time = time.monotonic_ns()
        stub = service.MessagingServiceStub(self.channel)
        return await stub.ForwardMessageToDeadLetterQueue(
            request, timeout=timeout_seconds
        )

    async def end_transaction(self, request, timeout_seconds: int):
        self.activity_nano_time = time.monotonic_ns()
        stub = service.MessagingServiceStub(self.channel)
        return await stub.EndTransaction(request, timeout=timeout_seconds)

    async def notify_client_termination(self, request, timeout_seconds: int):
        self.activity_nano_time = time.monotonic_ns()
        stub = service.MessagingServiceStub(self.channel)
        return await stub.NotifyClientTermination(request, timeout=timeout_seconds)

    # TODO: Not yet implemented
    async def telemetry(self, metadata, duration, response_observer):
        pass
