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

from rocketmq.rpc_client import Endpoints, RpcClient
from rocketmq.client import Client
from protocol import service_pb2
import threading


class ClientManager:
    def __init__(self, client: Client):
        self.__client = client
        self.__rpc_clients = {}
        self.__rpc_clients_lock = threading.Lock()

    def __get_rpc_client(self, endpoints: Endpoints, ssl_enabled: bool) -> RpcClient:
        with self.__rpc_clients_lock:
            rpc_client = self.__rpc_clients.get(endpoints)
            if rpc_client:
                return rpc_client
            rpc_client = RpcClient(endpoints.get_target(), ssl_enabled)
            self.__rpc_clients[endpoints] = rpc_client
            return rpc_client

    async def query_route(
        self,
        endpoints: Endpoints,
        request: service_pb2.QueryRouteRequest,
        timeout_seconds: int,
    ):
        rpc_client = self.__get_rpc_client(
            endpoints, self.__client.client_config.ssl_enabled
        )
        return await rpc_client.query_route(request, timeout_seconds)

    async def heartbeat(
        self,
        endpoints: Endpoints,
        request: service_pb2.HeartbeatRequest,
        timeout_seconds: int,
    ):
        rpc_client = self.__get_rpc_client(
            endpoints, self.__client.client_config.ssl_enabled
        )
        return await rpc_client.heartbeat(request, timeout_seconds)

    async def send_message(
        self,
        endpoints: Endpoints,
        request: service_pb2.SendMessageRequest,
        timeout_seconds: int,
    ):
        rpc_client = self.__get_rpc_client(
            endpoints, self.__client.client_config.ssl_enabled
        )
        return await rpc_client.send_message(request, timeout_seconds)

    async def query_assignment(
        self,
        endpoints: Endpoints,
        request: service_pb2.QueryAssignmentRequest,
        timeout_seconds: int,
    ):
        rpc_client = self.__get_rpc_client(
            endpoints, self.__client.client_config.ssl_enabled
        )
        return await rpc_client.query_assignment(request, timeout_seconds)

    async def ack_message(
        self,
        endpoints: Endpoints,
        request: service_pb2.AckMessageRequest,
        timeout_seconds: int,
    ):
        rpc_client = self.__get_rpc_client(
            endpoints, self.__client.client_config.ssl_enabled
        )
        return await rpc_client.ack_message(request, timeout_seconds)

    async def forward_message_to_dead_letter_queue(
        self,
        endpoints: Endpoints,
        request: service_pb2.ForwardMessageToDeadLetterQueueRequest,
        timeout_seconds: int,
    ):
        rpc_client = self.__get_rpc_client(
            endpoints, self.__client.client_config.ssl_enabled
        )
        return await rpc_client.forward_message_to_dead_letter_queue(
            request, timeout_seconds
        )

    async def end_transaction(
        self,
        endpoints: Endpoints,
        request: service_pb2.EndTransactionRequest,
        timeout_seconds: int,
    ):
        rpc_client = self.__get_rpc_client(
            endpoints, self.__client.client_config.ssl_enabled
        )
        return await rpc_client.end_transaction(request, timeout_seconds)

    async def notify_client_termination(
        self,
        endpoints: Endpoints,
        request: service_pb2.NotifyClientTerminationRequest,
        timeout_seconds: int,
    ):
        rpc_client = self.__get_rpc_client(
            endpoints, self.__client.client_config.ssl_enabled
        )
        return await rpc_client.notify_client_termination(request, timeout_seconds)

    async def change_invisible_duration(
        self,
        endpoints: Endpoints,
        request: service_pb2.ChangeInvisibleDurationRequest,
        timeout_seconds: int,
    ):
        rpc_client = self.__get_rpc_client(
            endpoints, self.__client.client_config.ssl_enabled
        )
        return await rpc_client.change_invisible_duration(request, timeout_seconds)
