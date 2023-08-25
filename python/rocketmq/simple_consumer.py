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
import random
import re
import threading
from datetime import timedelta
from threading import Lock
from typing import Dict

import rocketmq
from google.protobuf.duration_pb2 import Duration
from rocketmq.client_config import ClientConfig
from rocketmq.consumer import Consumer
from rocketmq.definition import PermissionHelper
from rocketmq.filter_expression import FilterExpression
from rocketmq.log import logger
from rocketmq.message import MessageView
from rocketmq.protocol.definition_pb2 import Resource
from rocketmq.protocol.definition_pb2 import Resource as ProtoResource
from rocketmq.protocol.service_pb2 import \
    AckMessageEntry as ProtoAckMessageEntry
from rocketmq.protocol.service_pb2 import \
    AckMessageRequest as ProtoAckMessageRequest
from rocketmq.protocol.service_pb2 import \
    ChangeInvisibleDurationRequest as ProtoChangeInvisibleDurationRequest
from rocketmq.rpc_client import Endpoints
from rocketmq.session_credentials import (SessionCredentials,
                                          SessionCredentialsProvider)
from rocketmq.simple_subscription_settings import SimpleSubscriptionSettings
from rocketmq.state import State
from utils import get_positive_mod


class SubscriptionLoadBalancer:
    """This class serves as a load balancer for message subscription.
    It keeps track of a rotating index to help distribute the load evenly.
    """

    def __init__(self, topic_route_data):
        #: current index for message queue selection
        self._index = random.randint(0, 10000)  # assuming a range of 0-10000
        #: thread lock to ensure atomic update to the index
        self._index_lock = threading.Lock()

        #: filter the message queues which are readable and from the master broker
        self._message_queues = [
            mq for mq in topic_route_data.message_queues
            if PermissionHelper().is_readable(mq.permission)
            and mq.broker.id == rocketmq.utils.master_broker_id
        ]

    def update(self, topic_route_data):
        """Updates the message queues based on the new topic route data."""
        self._index += 1
        self._message_queues = [
            mq for mq in topic_route_data.message_queues
            if PermissionHelper().is_readable(mq.permission)
            and mq.broker.id == rocketmq.utils.master_broker_id
        ]
        return self

    def take_message_queue(self):
        """Fetches the next message queue based on the current index."""
        with self._index_lock:
            index = get_positive_mod(self._index, len(self._message_queues))
            self._index += 1
        return self._message_queues[index]


class SimpleConsumer(Consumer):
    """The SimpleConsumer class extends the Client class and is used to consume
    messages from specific topics in RocketMQ.
    """

    def __init__(self, client_config: ClientConfig, consumer_group: str, await_duration: int, subscription_expressions: Dict[str, FilterExpression]):
        """Create a new SimpleConsumer.

        :param client_config: The configuration for the client.
        :param consumer_group: The consumer group.
        :param await_duration: The await duration.
        :param subscription_expressions: The subscription expressions.
        """
        super().__init__(client_config, consumer_group)

        self._consumer_group = consumer_group
        self._await_duration = await_duration
        self._subscription_expressions = subscription_expressions

        self._simple_subscription_settings = SimpleSubscriptionSettings(self.client_id, self.endpoints, self._consumer_group, timedelta(seconds=10), 10, self._subscription_expressions)
        self._subscription_route_data_cache = {}
        self._topic_round_robin_index = 0
        self._state_lock = Lock()
        self._state = State.New
        self._subscription_load_balancer = {}  # A dictionary to keep subscription load balancers

    def get_topics(self):
        return set(self._subscription_expressions.keys())

    def get_settings(self):
        return self._simple_subscription_settings

    async def subscribe(self, topic: str, filter_expression: FilterExpression):
        if self._state != State.Running:
            raise Exception("Simple consumer is not running")

        await self.get_subscription_load_balancer(topic)
        self._subscription_expressions[topic] = filter_expression

    def unsubscribe(self, topic: str):
        if self._state != State.Running:
            raise Exception("Simple consumer is not running")
        try:
            self._subscription_expressions.pop(topic)
        except KeyError:
            pass

    async def start(self):
        """Start the RocketMQ consumer and log the operation."""
        logger.info(f"Begin to start the rocketmq consumer, client_id={self.client_id}")
        with self._state_lock:
            if self._state != State.New:
                raise Exception("Consumer already started")
            await super().start()
            # Start all necessary operations
            self._state = State.Running
        logger.info(f"The rocketmq consumer starts successfully, client_id={self.client_id}")

    async def shutdown(self):
        """Shutdown the RocketMQ consumer and log the operation."""
        logger.info(f"Begin to shutdown the rocketmq consumer, client_id={self.client_id}")
        with self._state_lock:
            if self._state != State.Running:
                raise Exception("Consumer is not running")
            # Shutdown all necessary operations
            self._state = State.Terminated
        await super().shutdown()
        logger.info(f"Shutdown the rocketmq consumer successfully, client_id={self.client_id}")

    def update_subscription_load_balancer(self, topic, topic_route_data):
        # if a load balancer for this topic already exists in the subscription routing data cache, update it
        subscription_load_balancer = self._subscription_route_data_cache.get(topic)
        if subscription_load_balancer:
            subscription_load_balancer.update(topic_route_data)
        # otherwise, create a new subscription load balancer
        else:
            subscription_load_balancer = SubscriptionLoadBalancer(topic_route_data)

        # store new or updated subscription load balancers in the subscription routing data cache
        self._subscription_route_data_cache[topic] = subscription_load_balancer
        return subscription_load_balancer

    async def get_subscription_load_balancer(self, topic):
        # if a load balancer for this topic already exists in the subscription routing data cache, return it
        subscription_load_balancer = self._subscription_route_data_cache.get(topic)
        if subscription_load_balancer:
            return subscription_load_balancer

        # otherwise, obtain the routing data for the topic
        topic_route_data = await self.get_route_data(topic)
        # update subscription load balancer
        return self.update_subscription_load_balancer(topic, topic_route_data)

    async def receive(self, max_message_num, invisible_duration):
        if self._state != State.Running:
            raise Exception("Simple consumer is not running")
        if max_message_num <= 0:
            raise Exception("maxMessageNum must be greater than 0")
        copy = dict(self._subscription_expressions)
        topics = list(copy.keys())
        if len(topics) == 0:
            raise ValueError("There is no topic to receive message")

        index = (self._topic_round_robin_index + 1) % len(topics)
        self._topic_round_robin_index = index
        topic = topics[index]
        filter_expression = self._subscription_expressions[topic]
        subscription_load_balancer = await self.get_subscription_load_balancer(topic)
        mq = subscription_load_balancer.take_message_queue()
        request = self.wrap_receive_message_request(max_message_num, mq, filter_expression, self._await_duration, invisible_duration)
        result = await self.receive_message(request, mq, self._await_duration)
        return result.messages

    def wrap_change_invisible_duration(self, message_view: MessageView, invisible_duration):
        topic_resource = ProtoResource()
        topic_resource.name = message_view.topic

        request = ProtoChangeInvisibleDurationRequest()
        request.topic.CopyFrom(topic_resource)
        group = ProtoResource()
        group.name = message_view.message_group
        logger.debug(message_view.message_group)
        request.group.CopyFrom(group)
        request.receipt_handle = message_view.receipt_handle
        request.invisible_duration.CopyFrom(Duration(seconds=invisible_duration))
        request.message_id = message_view.message_id

        return request

    async def change_invisible_duration(self, message_view: MessageView, invisible_duration):
        if self._state != State.Running:
            raise Exception("Simple consumer is not running")

        request = self.wrap_change_invisible_duration(message_view, invisible_duration)
        result = await self.client_manager.change_invisible_duration(
            message_view.message_queue.broker.endpoints,
            request,
            self.client_config.request_timeout
        )
        logger.debug(result)

    async def ack(self, message_view: MessageView):
        if self._state != State.Running:
            raise Exception("Simple consumer is not running")
        request = self.wrap_ack_message_request(message_view)
        result = await self.client_manager.ack_message(message_view.message_queue.broker.endpoints, request=request, timeout_seconds=self.client_config.request_timeout)
        logger.info(result)

    def get_protobuf_group(self):
        return ProtoResource(name=self.consumer_group)

    def wrap_ack_message_request(self, message_view: MessageView):
        topic_resource = ProtoResource()
        topic_resource.name = message_view.topic
        entry = ProtoAckMessageEntry()
        entry.message_id = message_view.message_id
        entry.receipt_handle = message_view.receipt_handle

        request = ProtoAckMessageRequest(group=self.get_protobuf_group(), topic=topic_resource, entries=[entry])
        return request

    class Builder:
        def __init__(self):
            self._consumer_group_regex = re.compile(r"^[%a-zA-Z0-9_-]+$")
            self._clientConfig = None
            self._consumerGroup = None
            self._awaitDuration = None
            self._subscriptionExpressions = {}

        def set_client_config(self, client_config: ClientConfig):
            if client_config is None:
                raise ValueError("clientConfig should not be null")
            self._clientConfig = client_config
            return self

        def set_consumer_group(self, consumer_group: str):
            if consumer_group is None:
                raise ValueError("consumerGroup should not be null")
            # Assuming CONSUMER_GROUP_REGEX is defined in the outer scope
            if not re.match(self._consumer_group_regex, consumer_group):
                raise ValueError(f"topic does not match the regex {self._consumer_group_regex}")
            self._consumerGroup = consumer_group
            return self

        def set_await_duration(self, await_duration: int):
            self._awaitDuration = await_duration
            return self

        def set_subscription_expression(self, subscription_expressions: Dict[str, FilterExpression]):
            if subscription_expressions is None:
                raise ValueError("subscriptionExpressions should not be null")
            if len(subscription_expressions) == 0:
                raise ValueError("subscriptionExpressions should not be empty")
            self._subscriptionExpressions = subscription_expressions
            return self

        async def build(self):
            if self._clientConfig is None:
                raise ValueError("clientConfig has not been set yet")
            if self._consumerGroup is None:
                raise ValueError("consumerGroup has not been set yet")
            if len(self._subscriptionExpressions) == 0:
                raise ValueError("subscriptionExpressions has not been set yet")

            simple_consumer = SimpleConsumer(self._clientConfig, self._consumerGroup, self._awaitDuration, self._subscriptionExpressions)
            await simple_consumer.start()
            return simple_consumer


async def test():
    credentials = SessionCredentials("username", "password")
    credentials_provider = SessionCredentialsProvider(credentials)
    client_config = ClientConfig(
        endpoints=Endpoints("endpoint"),
        session_credentials_provider=credentials_provider,
        ssl_enabled=True,
    )
    topic = Resource()
    topic.name = "normal_topic"

    consumer_group = "yourConsumerGroup"
    subscription = {topic.name: FilterExpression("*")}
    simple_consumer = (await SimpleConsumer.Builder()
                       .set_client_config(client_config)
                       .set_consumer_group(consumer_group)
                       .set_await_duration(15)
                       .set_subscription_expression(subscription)
                       .build())
    logger.info(simple_consumer)
    # while True:
    message_views = await simple_consumer.receive(16, 15)
    logger.info(message_views)
    for message in message_views:
        logger.info(message.body)
        logger.info(f"Received a message, topic={message.topic}, message-id={message.message_id}, body-size={len(message.body)}")
        await simple_consumer.ack(message)
        logger.info(f"Message is acknowledged successfully, message-id={message.message_id}")


async def test_fifo_message():
    credentials = SessionCredentials("username", "password")
    credentials_provider = SessionCredentialsProvider(credentials)
    client_config = ClientConfig(
        endpoints=Endpoints("endpoint"),
        session_credentials_provider=credentials_provider,
        ssl_enabled=True,
    )
    topic = Resource()
    topic.name = "fifo_topic"

    consumer_group = "yourConsumerGroup"
    subscription = {topic.name: FilterExpression("*")}
    simple_consumer = (await SimpleConsumer.Builder()
                       .set_client_config(client_config)
                       .set_consumer_group(consumer_group)
                       .set_await_duration(15)
                       .set_subscription_expression(subscription)
                       .build())
    logger.info(simple_consumer)
    # while True:
    message_views = await simple_consumer.receive(16, 15)
    # logger.info(message_views)
    for message in message_views:
        logger.info(message.body)
        logger.info(f"Received a message, topic={message.topic}, message-id={message.message_id}, body-size={len(message.body)}")
        await simple_consumer.ack(message)
        logger.info(f"Message is acknowledged successfully, message-id={message.message_id}")


async def test_change_invisible_duration():
    credentials = SessionCredentials("username", "password")
    credentials_provider = SessionCredentialsProvider(credentials)
    client_config = ClientConfig(
        endpoints=Endpoints("endpoint"),
        session_credentials_provider=credentials_provider,
        ssl_enabled=True,
    )
    topic = Resource()
    topic.name = "fifo_topic"

    consumer_group = "yourConsumerGroup"
    subscription = {topic.name: FilterExpression("*")}
    simple_consumer = (await SimpleConsumer.Builder()
                       .set_client_config(client_config)
                       .set_consumer_group(consumer_group)
                       .set_await_duration(15)
                       .set_subscription_expression(subscription)
                       .build())
    logger.info(simple_consumer)
    # while True:
    message_views = await simple_consumer.receive(16, 15)
    # logger.info(message_views)
    for message in message_views:
        await simple_consumer.change_invisible_duration(message_view=message, invisible_duration=3)
        logger.info(message.body)
        logger.info(f"Received a message, topic={message.topic}, message-id={message.message_id}, body-size={len(message.body)}")
        await simple_consumer.ack(message)
        logger.info(f"Message is acknowledged successfully, message-id={message.message_id}")


async def test_subscribe_unsubscribe():
    credentials = SessionCredentials("username", "password")
    credentials_provider = SessionCredentialsProvider(credentials)
    client_config = ClientConfig(
        endpoints=Endpoints("endpoint"),
        session_credentials_provider=credentials_provider,
        ssl_enabled=True,
    )
    topic = Resource()
    topic.name = "normal_topic"

    consumer_group = "yourConsumerGroup"
    subscription = {topic.name: FilterExpression("*")}
    simple_consumer = (await SimpleConsumer.Builder()
                       .set_client_config(client_config)
                       .set_consumer_group(consumer_group)
                       .set_await_duration(15)
                       .set_subscription_expression(subscription)
                       .build())
    logger.info(simple_consumer)
    # while True:
    message_views = await simple_consumer.receive(16, 15)
    logger.info(message_views)
    for message in message_views:
        logger.info(message.body)
        logger.info(f"Received a message, topic={message.topic}, message-id={message.message_id}, body-size={len(message.body)}")
        await simple_consumer.ack(message)
        logger.info(f"Message is acknowledged successfully, message-id={message.message_id}")
    simple_consumer.unsubscribe('normal_topic')
    await simple_consumer.subscribe('fifo_topic', FilterExpression("*"))
    message_views = await simple_consumer.receive(16, 15)
    logger.info(message_views)
    for message in message_views:
        logger.info(message.body)
        logger.info(f"Received a message, topic={message.topic}, message-id={message.message_id}, body-size={len(message.body)}")
        await simple_consumer.ack(message)
        logger.info(f"Message is acknowledged successfully, message-id={message.message_id}")

if __name__ == "__main__":
    asyncio.run(test_subscribe_unsubscribe())
