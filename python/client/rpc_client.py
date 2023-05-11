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

import protocol.service_pb2_grpc as servicegrpc
from grpc import insecure_channel, ssl_channel_credentials
from grpc_interceptor import ClientCallDetails, ClientInterceptor


class MetadataInterceptor(ClientInterceptor):
    def __init__(self, metadata):
        self.metadata = metadata

    def intercept(self, request, metadata, client_call_details, next):
        metadata.update(self.metadata)
        new_client_call_details = ClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            metadata,
            client_call_details.credentials,
            client_call_details.wait_for_ready,
            client_call_details.compression,
        )
        return next(request, new_client_call_details)


class RpcClient:
    CONNECT_TIMEOUT_MILLIS = 3*1000
    GRPC_MAX_MESSAGE_SIZE = 2*31 - 1

    def __init__(self, endpoints, sslEnabled):
        channel_options = [
            ('grpc.max_send_message_length', -1),
            ('grpc.max_receive_message_length', -1),
            ('grpc.keepalive_time_ms', 1000),
            ('grpc.keepalive_timeout_ms', 5000),
            ('grpc.keepalive_permit_without_calls', True),
            ('grpc.connect_timeout_ms', self.CONNECT_TIMEOUT_MILLIS),
        ]
        if sslEnabled:
            ssl_creds = ssl_channel_credentials()
            self.channel = Channel(endpoints.getGrpcTarget(), ssl_creds, options=channel_options)
        else:
            self.channel = insecure_channel(endpoints.getGrpcTarget(), options=channel_options)

        self.activityNanoTime = time.monotonic_ns()

    def get_stub(self, metadata):
        interceptor = MetadataInterceptor(metadata)

        interceptor_channel = grpc.intercept_channel(self.channel, interceptor)
        stub = servicegrpc.MessagingServiceStub(interceptor_channel)
        return stub

    def __del__(self):
        self.channel.close()
    
    def idle_duration(activity_nano_time):
        return timedelta(microseconds=(time.monotonic_ns() - activity_nano_time) / 1000)
    
    async def query_route(self, metadata, request, duration):
        self.activity_nano_time = time.monotonic_ns()
        stub = self.get_stub(self, metadata)
        return await stub.QueryRoute(request, timeout=duration)

    async def heartbeat(self, metadata, request, duration):
        self.activity_nano_time = time.monotonic_ns()
        stub = self.get_stub(self, metadata)
        return await stub.Heartbeat(request, timeout=duration)

    async def send_message(self, metadata, request, duration):
        self.activity_nano_time = time.monotonic_ns()
        stub = self.get_stub(self, metadata)
        return await stub.SendMessage(request, timeout=duration)

    async def query_assignment(self, metadata, request, duration):
        self.activity_nano_time = time.monotonic_ns()
        stub = self.get_stub(self, metadata)
        return await stub.QueryAssignment(request, timeout=duration)

    # TODO: Not yet imeplemented
    async def receive_message(self, metadata, request, duration):
        pass

    async def ack_message(self, metadata, request, duration):
        self.activity_nano_time = time.monotonic_ns()
        stub = self.get_stub(self, metadata)
        return await stub.AckMessage(request, timeout=duration)

    async def change_invisible_duration(self, metadata, request, duration):
        self.activity_nano_time = time.monotonic_ns()
        stub = self.get_stub(self, metadata)
        return await stub.ChangeInvisibleDuration(request, timeout=duration)

    async def forward_message_to_dead_letter_queue(self, metadata, request, duration):
        self.activity_nano_time = time.monotonic_ns()
        stub = self.get_stub(self, metadata)
        return await stub.ForwardMessageToDeadLetterQueue(request, timeout=duration)
    
    async def endTransaction(self, metadata, request, duration):
        self.activity_nano_time = time.monotonic_ns()
        stub = self.get_stub(self, metadata)
        return await stub.EndTransaction(request, timeout=duration)
    

    async def notifyClientTermination(self, metadata, request, duration):
        self.activity_nano_time = time.monotonic_ns()
        stub = self.get_stub(self, metadata)
        return await stub.NotifyClientTermination(request, timeout=duration)

    async def telemetry(self, metadata, duration, response_observer):
        stub = self.get_stub(self, metadata)
        return await stub.Telemetry(response_observer, timeout=duration)