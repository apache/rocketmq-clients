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
import threading

from protocol import definition_pb2, service_pb2
from protocol.definition_pb2 import Code as ProtoCode
from protocol.service_pb2 import HeartbeatRequest, QueryRouteRequest
from rocketmq.client_config import ClientConfig
from rocketmq.client_id_encoder import ClientIdEncoder
from rocketmq.definition import Resource, TopicRouteData
from rocketmq.log import logger
from rocketmq.rpc_client import Endpoints, RpcClient
from rocketmq.session import Session
from rocketmq.signature import Signature


class ScheduleWithFixedDelay:
    def __init__(self, action, delay, period):
        self.action = action
        self.delay = delay
        self.period = period
        self.task = None

    async def start(self):
        await asyncio.sleep(self.delay)
        while True:
            try:
                await self.action()
            except Exception as e:
                logger.error(e, "Failed to execute scheduled task")
            finally:
                await asyncio.sleep(self.period)

    def schedule(self):
        loop1 = asyncio.new_event_loop()
        asyncio.set_event_loop(loop1)
        self.task = asyncio.create_task(self.start())

    def cancel(self):
        if self.task:
            self.task.cancel()


class Client:
    """
    Main client class which handles interaction with the server.
    """
    def __init__(self, client_config: ClientConfig):
        """
        Initialization method for the Client class.

        :param client_config: Client configuration.
        :param topics: Set of topics that the client is subscribed to.
        """
        self.client_config = client_config
        self.client_id = ClientIdEncoder.generate()
        self.endpoints = client_config.endpoints

        #: A cache to store topic routes.
        self.topic_route_cache = {}

        #: A table to store session information.
        self.sessions_table = {}
        self.sessionsLock = threading.Lock()
        self.client_manager = ClientManager(self)

        #: A dictionary to store isolated items.
        self.isolated = dict()

    def get_topics(self):
        raise NotImplementedError("This method should be implemented by the subclass.")

    async def start(self):
        """
        Start method which initiates fetching of topic routes and schedules heartbeats.
        """
        # get topic route
        logger.debug(f"Begin to start the rocketmq client, client_id={self.client_id}")
        for topic in self.get_topics():
            self.topic_route_cache[topic] = await self.fetch_topic_route(topic)
        scheduler = ScheduleWithFixedDelay(self.heartbeat, 3, 12)
        scheduler_sync_settings = ScheduleWithFixedDelay(self.sync_settings, 3, 12)
        scheduler.schedule()
        scheduler_sync_settings.schedule()
        logger.debug(f"Start the rocketmq client successfully, client_id={self.client_id}")

    async def shutdown(self):
        logger.debug(f"Begin to shutdown rocketmq client, client_id={self.client_id}")

        logger.debug(f"Shutdown the rocketmq client successfully, client_id={self.client_id}")

    async def heartbeat(self):
        """
        Asynchronous method that sends a heartbeat to the server.
        """
        try:
            endpoints = self.get_total_route_endpoints()
            request = HeartbeatRequest()
            request.client_type = definition_pb2.PRODUCER
            topic = Resource()
            topic.name = "normal_topic"
            # Collect task into a map.
            for item in endpoints:
                try:

                    task = await self.client_manager.heartbeat(item, request, self.client_config.request_timeout)
                    code = task.status.code
                    if code == ProtoCode.OK:
                        logger.info(f"Send heartbeat successfully, endpoints={item}, client_id={self.client_id}")

                        if item in self.isolated:
                            self.isolated.pop(item)
                            logger.info(f"Rejoin endpoints which was isolated before, endpoints={item}, "
                                        + f"client_id={self.client_id}")
                        return
                    status_message = task.status.message
                    logger.info(f"Failed to send heartbeat, endpoints={item}, code={code}, "
                                + f"status_message={status_message}, client_id={self.client_id}")
                except Exception:
                    logger.error(f"Failed to send heartbeat, endpoints={item}")
        except Exception as e:
            logger.error(f"[Bug] unexpected exception raised during heartbeat, client_id={self.client_id}, Exception: {str(e)}")

    def get_total_route_endpoints(self):
        """
        Method that returns all route endpoints.
        """
        endpoints = set()
        for item in self.topic_route_cache.items():
            for endpoint in [mq.broker.endpoints for mq in item[1].message_queues]:
                endpoints.add(endpoint)
        return endpoints

    async def get_route_data(self, topic):
        """
        Asynchronous method that fetches route data for a given topic.

        :param topic: The topic to fetch route data for.
        """
        if topic in self.topic_route_cache:
            return self.topic_route_cache[topic]
        topic_route_data = await self.fetch_topic_route(topic=topic)
        return topic_route_data

    def get_client_config(self):
        """
        Method to return client configuration.
        """
        return self.client_config

    async def sync_settings(self):
        total_route_endpoints = self.get_total_route_endpoints()

        for endpoints in total_route_endpoints:
            created, session = await self.get_session(endpoints)
            await session.sync_settings(True)
            logger.info(f"Sync settings to remote, endpoints={endpoints}")

    def stats(self):
        # TODO: stats implement
        pass

    async def notify_client_termination(self):
        pass

    async def on_recover_orphaned_transaction_command(self, endpoints, command):
        pass

    async def on_verify_message_command(self, endpoints, command):
        logger.warn(f"Ignore verify message command from remote, which is not expected, clientId={self.client_id}, "
                    + f"endpoints={endpoints}, command={command}")
        pass

    async def on_print_thread_stack_trace_command(self, endpoints, command):
        pass

    async def on_settings_command(self, endpoints, settings):
        pass

    async def on_topic_route_data_fetched(self, topic, topic_route_data):
        """
        Asynchronous method that handles the process once the topic route data is fetched.

        :param topic: The topic for which the route data is fetched.
        :param topic_route_data: The fetched topic route data.
        """
        route_endpoints = set()
        for mq in topic_route_data.message_queues:
            route_endpoints.add(mq.broker.endpoints)

        existed_route_endpoints = self.get_total_route_endpoints()
        new_endpoints = route_endpoints.difference(existed_route_endpoints)

        for endpoints in new_endpoints:
            created, session = await self.get_session(endpoints)
            if not created:
                continue
            logger.info(f"Begin to establish session for endpoints={endpoints}, client_id={self.client_id}")
            await session.sync_settings(True)
            logger.info(f"Establish session for endpoints={endpoints} successfully, client_id={self.client_id}")

        self.topic_route_cache[topic] = topic_route_data

    async def fetch_topic_route0(self, topic):
        """
        Asynchronous method that fetches the topic route.

        :param topic: The topic to fetch the route for.
        """
        try:
            req = QueryRouteRequest()
            req.topic.name = topic
            address = req.endpoints.addresses.add()
            address.host = self.endpoints.Addresses[0].host
            address.port = self.endpoints.Addresses[0].port
            req.endpoints.scheme = self.endpoints.scheme.to_protobuf(self.endpoints.scheme)
            response = await self.client_manager.query_route(self.endpoints, req, 10)
            code = response.status.code
            if code != ProtoCode.OK:
                logger.error(f"Failed to fetch topic route, client_id={self.client_id}, topic={topic}, code={code}, "
                             + f"statusMessage={response.status.message}")
            message_queues = response.message_queues
            return TopicRouteData(message_queues)
        except Exception as e:
            logger.error(e, f"Failed to fetch topic route, client_id={self.client_id}, topic={topic}")
            raise

    async def fetch_topic_route(self, topic):
        """
        Asynchronous method that fetches the topic route and updates the data.

        :param topic: The topic to fetch the route for.
        """
        topic_route_data = await self.fetch_topic_route0(topic)
        await self.on_topic_route_data_fetched(topic, topic_route_data)
        logger.info(f"Fetch topic route successfully, client_id={self.client_id}, topic={topic}, topicRouteData={topic_route_data}")
        return topic_route_data

    async def get_session(self, endpoints):
        """
        Asynchronous method that gets the session for a given endpoint.

        :param endpoints: The endpoints to get the session for.
        """
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

            stream = self.client_manager.telemetry(endpoints, 10000000)
            created = Session(endpoints, stream, self)
            self.sessions_table[endpoints] = created
            return (True, created)
        finally:
            self.sessionsLock.release()

    def get_client_id(self):
        return self.client_id


class ClientManager:
    """Manager class for RPC Clients in a thread-safe manner.
    Each instance is created by a specific client and can manage
    multiple RPC clients.
    """

    def __init__(self, client: Client):
        #: The client that instantiated this manager.
        self.__client = client

        #: A dictionary that maps endpoints to the corresponding RPC clients.
        self.__rpc_clients = {}

        #: A lock used to ensure thread safety when accessing __rpc_clients.
        self.__rpc_clients_lock = threading.Lock()

    def __get_rpc_client(self, endpoints: Endpoints, ssl_enabled: bool):
        """Retrieve the RPC client corresponding to the given endpoints.
        If not present, a new RPC client is created and stored in __rpc_clients.

        :param endpoints: The endpoints associated with the RPC client.
        :param ssl_enabled: A flag indicating whether SSL is enabled.
        :return: The RPC client associated with the given endpoints.
        """
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
        """Query the routing information.

        :param endpoints: The endpoints to query.
        :param request: The request containing the details of the query.
        :param timeout_seconds: The maximum time to wait for a response.
        :return: The result of the query.
        """
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
        """Send a heartbeat to the server to indicate that the client is still alive.

        :param endpoints: The endpoints to send the heartbeat to.
        :param request: The request containing the details of the heartbeat.
        :param timeout_seconds: The maximum time to wait for a response.
        :return: The result of the heartbeat.
        """
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
        """Send a message to the server.

        :param endpoints: The endpoints to send the message to.
        :param request: The request containing the details of the message.
        :param timeout_seconds: The maximum time to wait for a response.
        :return: The result of the message sending operation.
        """
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
        """Query the assignment information.

        :param endpoints: The endpoints to query.
        :param request: The request containing the details of the query.
        :param timeout_seconds: The maximum time to wait for a response.
        :return: The result of the query.
        """
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
        """Send an acknowledgment for a message to the server.

        :param endpoints: The endpoints to send the acknowledgment to.
        :param request: The request containing the details of the acknowledgment.
        :param timeout_seconds: The maximum time to wait for a response.
        :return: The result of the acknowledgment.
        """
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
        """Forward a message to the dead letter queue.

        :param endpoints: The endpoints to send the request to.
        :param request: The request containing the details of the message to forward.
        :param timeout_seconds: The maximum time to wait for a response.
        :return: The result of the forward operation.
        """
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
        """Ends a transaction.

        :param endpoints: The endpoints to send the request to.
        :param request: The request to end the transaction.
        :param timeout_seconds: The maximum time to wait for a response.
        :return: The result of the end transaction operation.
        """
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
        """Notify server about client termination.

        :param endpoints: The endpoints to send the notification to.
        :param request: The request containing the details of the termination.
        :param timeout_seconds: The maximum time to wait for a response.
        :return: The result of the notification operation.
        """
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
        """Change the invisible duration of a message.

        :param endpoints: The endpoints to send the request to.
        :param request: The request containing the new invisible duration.
        :param timeout_seconds: The maximum time to wait for a response.
        :return: The result of the change operation.
        """
        rpc_client = self.__get_rpc_client(
            endpoints, self.__client.client_config.ssl_enabled
        )
        metadata = Signature.sign(self.__client.client_config, self.__client.client_id)
        return await rpc_client.change_invisible_duration(
            request, metadata, timeout_seconds
        )

    async def receive_message(
        self,
        endpoints: Endpoints,
        request: service_pb2.ReceiveMessageRequest,
        timeout_seconds: int,
    ):
        rpc_client = self.__get_rpc_client(
            endpoints, self.__client.client_config.ssl_enabled
        )
        metadata = Signature.sign(self.__client.client_config, self.__client.client_id)

        response = await rpc_client.receive_message(
            request, metadata, timeout_seconds
        )
        return response

    def telemetry(
        self,
        endpoints: Endpoints,
        timeout_seconds: int,
    ):
        """Fetch telemetry information.

        :param endpoints: The endpoints to send the request to.
        :param timeout_seconds: The maximum time to wait for a response.
        :return: The telemetry information.
        """
        rpc_client = self.__get_rpc_client(
            endpoints, self.__client.client_config.ssl_enabled
        )
        metadata = Signature.sign(self.__client.client_config, self.__client.client_id)
        return rpc_client.telemetry(metadata, timeout_seconds)
