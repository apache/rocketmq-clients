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

import threading
from typing import Set

from protocol import service_pb2
from protocol.service_pb2 import QueryRouteRequest
from rocketmq.client_config import ClientConfig
from rocketmq.client_id_encoder import ClientIdEncoder
from rocketmq.rpc_client import Endpoints, RpcClient
from rocketmq.session import Session
from rocketmq.signature import Signature
from rocketmq.topic_route_data import TopicRouteData


class Client:
    def __init__(self, client_config: ClientConfig, topics: Set[str]):
        self.client_config = client_config
        self.client_id = ClientIdEncoder.generate()
        self.endpoints = client_config.endpoints
        self.topics = topics

        self.topic_route_cache = {}

        self.sessions_table = {}
        self.sessionsLock = threading.Lock()
        self.client_manager = ClientManager(self)

    async def start_up(self):
        # get topic route
        for topic in self.topics:
            self.topic_route_cache[topic] = await self.fetch_topic_route(topic)

    def GetTotalRouteEndpoints(self):
        endpoints = set()
        for item in self.topic_route_cache.items():
            for endpoint in [mq.broker.endpoints for mq in item[1].message_queues]:
                endpoints.add(endpoint)
        return endpoints

    def get_client_config(self):
        return self.client_config

    async def OnTopicRouteDataFetched(self, topic, topicRouteData):
        route_endpoints = set()
        for mq in topicRouteData.message_queues:
            route_endpoints.add(mq.broker.endpoints)

        existed_route_endpoints = self.GetTotalRouteEndpoints()
        new_endpoints = route_endpoints.difference(existed_route_endpoints)

        for endpoints in new_endpoints:
            created, session = await self.GetSession(endpoints)
            if not created:
                continue

            await session.sync_settings(True)

        self.topic_route_cache[topic] = topicRouteData
        # self.OnTopicRouteDataUpdated0(topic, topicRouteData)

    async def fetch_topic_route0(self, topic):
        req = QueryRouteRequest()
        req.topic.name = topic
        address = req.endpoints.addresses.add()
        address.host = self.endpoints.Addresses[0].host
        address.port = self.endpoints.Addresses[0].port
        req.endpoints.scheme = self.endpoints.scheme.to_protobuf(self.endpoints.scheme)
        response = await self.client_manager.query_route(self.endpoints, req, 10)

        message_queues = response.message_queues
        return TopicRouteData(message_queues)

    # return topic data
    async def fetch_topic_route(self, topic):
        topic_route_data = await self.fetch_topic_route0(topic)
        await self.OnTopicRouteDataFetched(topic, topic_route_data)
        return topic_route_data

    async def GetSession(self, endpoints):
        self.sessionsLock.acquire()
        try:
            # Session exists, return in advance.
            if endpoints in self.sessions_table:
                return (False, self.sessions_table[endpoints])
        finally:
            self.sessionsLock.release()

        self.sessionsLock.acquire()
        try:
            # Session exists, return in advance.
            if endpoints in self.sessions_table:
                return (False, self.sessions_table[endpoints])

            stream = self.client_manager.telemetry(endpoints, 10)
            created = Session(endpoints, stream, self)
            self.sessions_table[endpoints] = created
            return (True, created)
        finally:
            self.sessionsLock.release()


class ClientManager:
    def __init__(self, client: Client):
        self.__client = client
        self.__rpc_clients = {}
        self.__rpc_clients_lock = threading.Lock()

    def __get_rpc_client(self, endpoints: Endpoints, ssl_enabled: bool):
        with self.__rpc_clients_lock:
            rpc_client = self.__rpc_clients.get(endpoints)
            if rpc_client:
                return rpc_client
            rpc_client = RpcClient(endpoints.grpc_target(True), ssl_enabled)
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

        metadata = Signature.sign(self.__client.client_config, self.__client.client_id)
        return await rpc_client.query_route(request, metadata, timeout_seconds)

    async def heartbeat(
        self,
        endpoints: Endpoints,
        request: service_pb2.HeartbeatRequest,
        timeout_seconds: int,
    ):
        rpc_client = self.__get_rpc_client(
            endpoints, self.__client.client_config.ssl_enabled
        )
        metadata = Signature.sign(self.__client.client_config, self.__client.client_id)
        return await rpc_client.heartbeat(request, metadata, timeout_seconds)

    async def send_message(
        self,
        endpoints: Endpoints,
        request: service_pb2.SendMessageRequest,
        timeout_seconds: int,
    ):
        rpc_client = self.__get_rpc_client(
            endpoints, self.__client.client_config.ssl_enabled
        )
        metadata = Signature.sign(self.__client.client_config, self.__client.client_id)
        return await rpc_client.send_message(request, metadata, timeout_seconds)

    async def query_assignment(
        self,
        endpoints: Endpoints,
        request: service_pb2.QueryAssignmentRequest,
        timeout_seconds: int,
    ):
        rpc_client = self.__get_rpc_client(
            endpoints, self.__client.client_config.ssl_enabled
        )
        metadata = Signature.sign(self.__client.client_config, self.__client.client_id)
        return await rpc_client.query_assignment(request, metadata, timeout_seconds)

    async def ack_message(
        self,
        endpoints: Endpoints,
        request: service_pb2.AckMessageRequest,
        timeout_seconds: int,
    ):
        rpc_client = self.__get_rpc_client(
            endpoints, self.__client.client_config.ssl_enabled
        )
        metadata = Signature.sign(self.__client.client_config, self.__client.client_id)
        return await rpc_client.ack_message(request, metadata, timeout_seconds)

    async def forward_message_to_dead_letter_queue(
        self,
        endpoints: Endpoints,
        request: service_pb2.ForwardMessageToDeadLetterQueueRequest,
        timeout_seconds: int,
    ):
        rpc_client = self.__get_rpc_client(
            endpoints, self.__client.client_config.ssl_enabled
        )
        metadata = Signature.sign(self.__client.client_config, self.__client.client_id)
        return await rpc_client.forward_message_to_dead_letter_queue(
            request, metadata, timeout_seconds
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
        metadata = Signature.sign(self.__client.client_config, self.__client.client_id)
        return await rpc_client.end_transaction(request, metadata, timeout_seconds)

    async def notify_client_termination(
        self,
        endpoints: Endpoints,
        request: service_pb2.NotifyClientTerminationRequest,
        timeout_seconds: int,
    ):
        rpc_client = self.__get_rpc_client(
            endpoints, self.__client.client_config.ssl_enabled
        )
        metadata = Signature.sign(self.__client.client_config, self.__client.client_id)
        return await rpc_client.notify_client_termination(
            request, metadata, timeout_seconds
        )

    async def change_invisible_duration(
        self,
        endpoints: Endpoints,
        request: service_pb2.ChangeInvisibleDurationRequest,
        timeout_seconds: int,
    ):
        rpc_client = self.__get_rpc_client(
            endpoints, self.__client.client_config.ssl_enabled
        )
        metadata = Signature.sign(self.__client.client_config, self.__client.client_id)
        return await rpc_client.change_invisible_duration(
            request, metadata, timeout_seconds
        )

    def telemetry(
        self,
        endpoints: Endpoints,
        timeout_seconds: int,
    ):
        rpc_client = self.__get_rpc_client(
            endpoints, self.__client.client_config.ssl_enabled
        )
        metadata = Signature.sign(self.__client.client_config, self.__client.client_id)
        return rpc_client.telemetry(metadata, timeout_seconds)
