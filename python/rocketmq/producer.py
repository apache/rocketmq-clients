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
import time
# from status_checker import StatusChecker
from datetime import datetime, timedelta
from threading import RLock
from typing import Set
from unittest.mock import MagicMock, patch

import rocketmq
from publishing_message import MessageType
from rocketmq.client import Client
from rocketmq.client_config import ClientConfig
from rocketmq.definition import PermissionHelper, TopicRouteData
from rocketmq.exponential_backoff_retry_policy import \
    ExponentialBackoffRetryPolicy
from rocketmq.log import logger
from rocketmq.message import Message
from rocketmq.message_id_codec import MessageIdCodec
from rocketmq.protocol.definition_pb2 import Message as ProtoMessage
from rocketmq.protocol.definition_pb2 import Resource
from rocketmq.protocol.definition_pb2 import Resource as ProtoResource
from rocketmq.protocol.definition_pb2 import SystemProperties
from rocketmq.protocol.definition_pb2 import \
    TransactionResolution as ProtoTransactionResolution
from rocketmq.protocol.service_pb2 import (EndTransactionRequest,
                                           SendMessageRequest)
from rocketmq.publish_settings import PublishingSettings
from rocketmq.publishing_message import PublishingMessage
from rocketmq.rpc_client import Endpoints
from rocketmq.send_receipt import SendReceipt
from rocketmq.session_credentials import (SessionCredentials,
                                          SessionCredentialsProvider)
from status_checker import TooManyRequestsException
from utils import get_positive_mod


class Transaction:
    MAX_MESSAGE_NUM = 1

    def __init__(self, producer):
        self.producer = producer
        self.messages = set()
        self.messages_lock = RLock()
        self.message_send_receipt_dict = {}

    def try_add_message(self, message):
        with self.messages_lock:
            if len(self.messages) > self.MAX_MESSAGE_NUM:
                raise ValueError(f"Message in transaction has exceed the threshold: {self.MAX_MESSAGE_NUM}")

            publishing_message = PublishingMessage(message, self.producer.publish_settings, True)
            self.messages.add(publishing_message)
            return publishing_message

    def try_add_receipt(self, publishing_message, send_receipt):
        with self.messages_lock:
            if publishing_message not in self.messages:
                raise ValueError("Message is not in the transaction")

            self.message_send_receipt_dict[publishing_message] = send_receipt

    async def commit(self):
        # if self.producer.state != "Running":
        #     raise Exception("Producer is not running")

        if not self.message_send_receipt_dict:
            raise ValueError("Transactional message has not been sent yet")

        for publishing_message, send_receipt in self.message_send_receipt_dict.items():
            await self.producer.end_transaction(send_receipt.endpoints, publishing_message.message.topic, send_receipt.message_id, send_receipt.transaction_id, "Commit")

    async def rollback(self):
        # if self.producer.state != "Running":
        #     raise Exception("Producer is not running")

        if not self.message_send_receipt_dict:
            raise ValueError("Transactional message has not been sent yet")

        for publishing_message, send_receipt in self.message_send_receipt_dict.items():
            await self.producer.end_transaction(send_receipt.endpoints, publishing_message.message.topic, send_receipt.message_id, send_receipt.transaction_id, "Rollback")


class PublishingLoadBalancer:
    """This class serves as a load balancer for message publishing.
    It keeps track of a rotating index to help distribute the load evenly.
    """

    def __init__(self, topic_route_data: TopicRouteData, index: int = 0):
        #: current index for message queue selection
        self.__index = index
        #: thread lock to ensure atomic update to the index
        self.__index_lock = threading.Lock()

        #: filter the message queues which are writable and from the master broker
        message_queues = []
        for mq in topic_route_data.message_queues:
            if (
                not PermissionHelper().is_writable(mq.permission)
                or mq.broker.id is not rocketmq.utils.master_broker_id
            ):
                continue
            message_queues.append(mq)
        self.__message_queues = message_queues

    @property
    def index(self):
        """Property to fetch the current index"""
        return self.__index

    def get_and_increment_index(self):
        """Thread safe method to get the current index and increment it by one"""
        with self.__index_lock:
            temp = self.__index
            self.__index += 1
            return temp

    def take_message_queues(self, excluded: Set[Endpoints], count: int):
        """Fetch a specified number of message queues, excluding the ones provided.
        It will first try to fetch from non-excluded brokers and if insufficient,
        it will select from the excluded ones.
        """
        next_index = self.get_and_increment_index()
        candidates = []
        candidate_broker_name = set()

        queue_num = len(self.__message_queues)
        for i in range(queue_num):
            mq = self.__message_queues[next_index % queue_num]
            next_index = next_index + 1
            if (
                mq.broker.endpoints not in excluded
                and mq.broker.name not in candidate_broker_name
            ):
                candidate_broker_name.add(mq.broker.name)
                candidates.append(mq)
            if len(candidates) >= count:
                return candidates
        # if all endpoints are isolated
        if candidates:
            return candidates
        for i in range(queue_num):
            mq = self.__message_queues[next_index % queue_num]
            if mq.broker.name not in candidate_broker_name:
                candidate_broker_name.add(mq.broker.name)
                candidates.append(mq)
            if len(candidates) >= count:
                return candidates
        return candidates

    def take_message_queue_by_message_group(self, message_group):
        index = get_positive_mod(hash(message_group), len(self.__message_queues))
        return self.__message_queues[index]


class Producer(Client):
    """The Producer class extends the Client class and is used to publish
    messages to specific topics in RocketMQ.
    """

    def __init__(self, client_config: ClientConfig, topics: Set[str]):
        """Create a new Producer.

        :param client_config: The configuration for the client.
        :param topics: The set of topics to which the producer can send messages.
        """
        super().__init__(client_config)
        self.publish_topics = topics
        retry_policy = ExponentialBackoffRetryPolicy.immediately_retry_policy(10)
        #: Set up the publishing settings with the given parameters.
        self.publish_settings = PublishingSettings(
            self.client_id, self.endpoints, retry_policy, 10, topics
        )
        #: Initialize the routedata cache.
        self.publish_routedata_cache = {}

    async def __aenter__(self):
        """Provide an asynchronous context manager for the producer."""
        await self.start()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Provide an asynchronous context manager for the producer."""
        await self.shutdown()

    def get_topics(self):
        return self.publish_topics

    async def start(self):
        """Start the RocketMQ producer and log the operation."""
        logger.info(f"Begin to start the rocketmq producer, client_id={self.client_id}")
        await super().start()
        logger.info(f"The rocketmq producer starts successfully, client_id={self.client_id}")

    async def shutdown(self):
        """Shutdown the RocketMQ producer and log the operation."""
        logger.info(f"Begin to shutdown the rocketmq producer, client_id={self.client_id}")
        await super().shutdown()
        logger.info(f"Shutdown the rocketmq producer successfully, client_id={self.client_id}")

    @staticmethod
    def wrap_send_message_request(message, message_queue):
        """Wrap the send message request for the RocketMQ producer.

        :param message: The message to be sent.
        :param message_queue: The queue to which the message will be sent.
        :return: The SendMessageRequest with the message and queue details.
        """
        req = SendMessageRequest()
        req.messages.extend([message.to_protobuf(message_queue.queue_id)])
        return req

    async def send(self, message, transaction: Transaction = None):
        tx_enabled = True
        if transaction is None:
            tx_enabled = False
        if tx_enabled:
            logger.debug("Transaction send")
            publishing_message = transaction.try_add_message(message)
            send_receipt = await self.send_message(message, tx_enabled)
            transaction.try_add_receipt(publishing_message, send_receipt)
            return send_receipt
        else:
            return await self.send_message(message)

    async def send_message(self, message, tx_enabled=False):
        """Send a message using a load balancer, retrying as needed according to the retry policy.

        :param message: The message to be sent.
        """
        publish_load_balancer = await self.get_publish_load_balancer(message.topic)
        publishing_message = PublishingMessage(message, self.publish_settings, tx_enabled)
        retry_policy = self.get_retry_policy()
        max_attempts = retry_policy.get_max_attempts()

        exception = None
        logger.debug(publishing_message.message.message_group)
        candidates = (
            publish_load_balancer.take_message_queues(set(self.isolated.keys()), max_attempts)
            if publishing_message.message.message_group is None else
            [publish_load_balancer.take_message_queue_by_message_group(publishing_message.message.message_group)])
        for attempt in range(1, max_attempts + 1):
            start_time = time.time()
            candidate_index = (attempt - 1) % len(candidates)
            mq = candidates[candidate_index]
            logger.debug(mq.accept_message_types)
            if self.publish_settings.is_validate_message_type() and publishing_message.message_type.value != mq.accept_message_types[0].value:
                raise ValueError(
                    "Current message type does not match with the accept message types,"
                    + f" topic={message.topic}, actualMessageType={publishing_message.message_type}"
                    + f" acceptMessageType={','}")

            send_message_request = self.wrap_send_message_request(publishing_message, mq)
            # topic_data = self.topic_route_cache["normal_topic"]
            endpoints = mq.broker.endpoints

            try:
                invocation = await self.client_manager.send_message(endpoints, send_message_request, self.client_config.request_timeout)
                logger.debug(invocation)
                send_recepits = SendReceipt.process_send_message_response(mq, invocation)
                send_recepit = send_recepits[0]
                if attempt > 1:
                    logger.info(
                        f"Re-send message successfully, topic={message.topic},"
                        + f" max_attempts={max_attempts}, endpoints={str(endpoints)}, clientId={self.client_id}")
                return send_recepit
            except Exception as e:
                exception = e
                self.isolated[endpoints] = True
                if attempt >= max_attempts:
                    logger.error("Failed to send message finally, run out of attempt times, "
                                 + f"topic={message.topic}, maxAttempt={max_attempts}, attempt={attempt}, "
                                 + f"endpoints={endpoints}, messageId={publishing_message.message_id}, clientId={self.client_id}")
                    raise
                if publishing_message.message_type == MessageType.TRANSACTION:
                    logger.error("Failed to send transaction message, run out of attempt times, "
                                 + f"topic={message.topic}, maxAttempt=1, attempt={attempt}, "
                                 + f"endpoints={endpoints}, messageId={publishing_message.message_id}, clientId={self.client_id}")
                    raise
                if not isinstance(exception, TooManyRequestsException):
                    logger.error(f"Failed to send message, topic={message.topic}, max_attempts={max_attempts}, "
                                 + f"attempt={attempt}, endpoints={endpoints}, messageId={publishing_message.message_id},"
                                 + f" clientId={self.client_id}")
                    continue

                nextAttempt = 1 + attempt
                delay = retry_policy.get_next_attempt_delay(nextAttempt)
                await asyncio.sleep(delay.total_seconds())
                logger.warning(f"Failed to send message due to too many requests, would attempt to resend after {delay},\
                                 topic={message.topic}, max_attempts={max_attempts}, attempt={attempt}, endpoints={endpoints},\
                                 message_id={publishing_message.message_id}, client_id={self.client_id}")
            finally:
                elapsed_time = time.time() - start_time
                logger.info(f"send time: {elapsed_time}")

    def update_publish_load_balancer(self, topic, topic_route_data):
        """Update the load balancer used for publishing messages to a topic.

        :param topic: The topic for which to update the load balancer.
        :param topic_route_data: The new route data for the topic.
        :return: The updated load balancer.
        """
        publishing_load_balancer = None
        if topic in self.publish_routedata_cache:
            publishing_load_balancer = self.publish_routedata_cache[topic]
        else:
            publishing_load_balancer = PublishingLoadBalancer(topic_route_data)
        self.publish_routedata_cache[topic] = publishing_load_balancer
        return publishing_load_balancer

    async def get_publish_load_balancer(self, topic):
        """Get the load balancer used for publishing messages to a topic.

        :param topic: The topic for which to get the load balancer.
        :return: The load balancer for the topic.
        """
        if topic in self.publish_routedata_cache:
            return self.publish_routedata_cache[topic]
        topic_route_data = await self.get_route_data(topic)
        return self.update_publish_load_balancer(topic, topic_route_data)

    def get_settings(self):
        """Get the publishing settings for this producer.

        :return: The publishing settings for this producer.
        """
        return self.publish_settings

    def get_retry_policy(self):
        """Get the retry policy for this producer.

        :return: The retry policy for this producer.
        """
        return self.publish_settings.GetRetryPolicy()

    def begin_transaction(self):
        """Start a new transaction."""
        return Transaction(self)

    async def end_transaction(self, endpoints, topic, message_id, transaction_id, resolution):
        """End a transaction based on its resolution (commit or rollback)."""
        topic_resource = ProtoResource(name=topic)
        request = EndTransactionRequest(
            transaction_id=transaction_id,
            message_id=message_id,
            topic=topic_resource,
            resolution=ProtoTransactionResolution.COMMIT if resolution == "Commit" else ProtoTransactionResolution.ROLLBACK
        )
        await self.client_manager.end_transaction(endpoints, request, self.client_config.request_timeout)
        # StatusChecker.check(invocation.response.status, request, invocation.request_id)


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
    msg = ProtoMessage()
    msg.topic.CopyFrom(topic)
    msg.body = b"My Normal Message Body"
    sysperf = SystemProperties()
    sysperf.message_id = MessageIdCodec.next_message_id()
    sysperf.message_group = "yourConsumerGroup"
    msg.system_properties.CopyFrom(sysperf)
    producer = Producer(client_config, topics={"normal_topic"})
    message = Message(topic.name, msg.body)
    await producer.start()
    await asyncio.sleep(10)
    send_receipt = await producer.send(message)
    logger.info(f"Send message successfully, {send_receipt}")


async def test_delay_message():
    credentials = SessionCredentials("username", "password")
    credentials_provider = SessionCredentialsProvider(credentials)
    client_config = ClientConfig(
        endpoints=Endpoints("endpoint"),
        session_credentials_provider=credentials_provider,
        ssl_enabled=True,
    )
    topic = Resource()
    topic.name = "delay_topic"
    msg = ProtoMessage()
    msg.topic.CopyFrom(topic)
    msg.body = b"My Delay Message Body"
    sysperf = SystemProperties()
    sysperf.message_id = MessageIdCodec.next_message_id()
    msg.system_properties.CopyFrom(sysperf)
    logger.debug(f"{msg}")
    producer = Producer(client_config, topics={"delay_topic"})
    current_time_millis = int(round(time.time() * 1000))
    message_delay_time = timedelta(seconds=10)
    result_time_millis = current_time_millis + int(message_delay_time.total_seconds() * 1000)
    result_time_datetime = datetime.fromtimestamp(result_time_millis / 1000.0)
    message = Message(topic.name, msg.body, delivery_timestamp=result_time_datetime)
    await producer.start()
    await asyncio.sleep(10)
    send_receipt = await producer.send(message)
    logger.info(f"Send message successfully, {send_receipt}")


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
    msg = ProtoMessage()
    msg.topic.CopyFrom(topic)
    msg.body = b"My FIFO Message Body"
    sysperf = SystemProperties()
    sysperf.message_id = MessageIdCodec.next_message_id()
    msg.system_properties.CopyFrom(sysperf)
    logger.debug(f"{msg}")
    producer = Producer(client_config, topics={"fifo_topic"})
    message = Message(topic.name, msg.body, message_group="yourConsumerGroup")
    await producer.start()
    await asyncio.sleep(10)
    send_receipt = await producer.send(message)
    logger.info(f"Send message successfully, {send_receipt}")


async def test_transaction_message():
    credentials = SessionCredentials("username", "password")
    credentials_provider = SessionCredentialsProvider(credentials)
    client_config = ClientConfig(
        endpoints=Endpoints("endpoint"),
        session_credentials_provider=credentials_provider,
        ssl_enabled=True,
    )
    topic = Resource()
    topic.name = "transaction_topic"
    msg = ProtoMessage()
    msg.topic.CopyFrom(topic)
    msg.body = b"My Transaction Message Body"
    sysperf = SystemProperties()
    sysperf.message_id = MessageIdCodec.next_message_id()
    msg.system_properties.CopyFrom(sysperf)
    logger.debug(f"{msg}")
    producer = Producer(client_config, topics={"transaction_topic"})
    message = Message(topic.name, msg.body)
    await producer.start()
    # await asyncio.sleep(10)
    transaction = producer.begin_transaction()
    send_receipt = await producer.send(message, transaction)
    logger.info(f"Send message successfully, {send_receipt}")
    await transaction.commit()


async def test_retry_and_isolation():
    credentials = SessionCredentials("username", "password")
    credentials_provider = SessionCredentialsProvider(credentials)
    client_config = ClientConfig(
        endpoints=Endpoints("endpoint"),
        session_credentials_provider=credentials_provider,
        ssl_enabled=True,
    )
    topic = Resource()
    topic.name = "normal_topic"
    msg = ProtoMessage()
    msg.topic.CopyFrom(topic)
    msg.body = b"My Message Body"
    sysperf = SystemProperties()
    sysperf.message_id = MessageIdCodec.next_message_id()
    msg.system_properties.CopyFrom(sysperf)
    logger.info(f"{msg}")
    producer = Producer(client_config, topics={"normal_topic"})
    message = Message(topic.name, msg.body)
    with patch.object(producer.client_manager, 'send_message', new_callable=MagicMock) as mock_send:
        mock_send.side_effect = Exception("Forced Exception for Testing")
        await producer.start()

        try:
            await producer.send(message)
        except Exception:
            logger.info("Exception occurred as expected")

        assert mock_send.call_count == producer.get_retry_policy().get_max_attempts(), "Number of attempts should equal max_attempts."
        logger.debug(producer.isolated)
        assert producer.isolated, "Endpoint should be marked as isolated after an error."

    logger.info("Test completed successfully.")

if __name__ == "__main__":
    asyncio.run(test())
    asyncio.run(test_delay_message())
    asyncio.run(test_fifo_message())
    asyncio.run(test_transaction_message())
    asyncio.run(test_retry_and_isolation())
