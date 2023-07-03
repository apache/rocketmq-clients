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
from typing import Set
import time
import rocketmq
from rocketmq.client import Client
from rocketmq.client_config import ClientConfig
from rocketmq.definition import TopicRouteData
from rocketmq.log import logger
from rocketmq.message_id_codec import MessageIdCodec
from rocketmq.protocol.definition_pb2 import Message as ProtoMessage
from rocketmq.protocol.definition_pb2 import Resource, SystemProperties
from rocketmq.protocol.service_pb2 import SendMessageRequest
from rocketmq.publish_settings import PublishingSettings
from rocketmq.exponential_backoff_retry_policy import ExponentialBackoffRetryPolicy
from rocketmq.rpc_client import Endpoints
from rocketmq.session_credentials import (SessionCredentials,
                                          SessionCredentialsProvider)


class PublishingLoadBalancer:
    def __init__(self, topic_route_data: TopicRouteData, index: int = 0):
        self.__index = index
        self.__index_lock = threading.Lock()
        message_queues = []
        for mq in topic_route_data.message_queues:
            if (
                not mq.permission.is_writable()
                or mq.broker.id is not rocketmq.utils.master_broker_id
            ):
                continue
            message_queues.append(mq)
        self.__message_queues = message_queues

    @property
    def index(self):
        return self.__index

    def get_and_increment_index(self):
        with self.__index_lock:
            temp = self.__index
            self.__index += 1
            return temp

    def take_message_queues(self, excluded: Set[Endpoints], count: int):
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


class Producer(Client):
    def __init__(self, client_config: ClientConfig, topics: Set[str]):
        super().__init__(client_config, topics)
        retry_policy = ExponentialBackoffRetryPolicy.immediately_retry_policy(10)
        self.publish_settings = PublishingSettings(
            self.client_id, self.endpoints, retry_policy, 10, topics
        )

    async def __aenter__(self):
        await self.start()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()

    async def start(self):
        logger.info(f"Begin to start the rocketmq producer, client_id={self.client_id}")
        await super().start()
        logger.info(f"The rocketmq producer starts successfully, client_id={self.client_id}")

    async def shutdown(self):
        logger.info(f"Begin to shutdown the rocketmq producer, client_id={self.client_id}")
        logger.info(f"Shutdown the rocketmq producer successfully, client_id={self.client_id}")


    async def send_message(self, message):
        retry_policy = self.get_retry_policy()
        maxAttempts = retry_policy.get_max_attempts()
        exception = None

        candidates = []
        for attempt in range(1, maxAttempts + 1):
            stopwatch_start = time.time()

            candidateIndex = (attempt - 1) % len(candidates)
            mq = candidates[candidateIndex]

            if self.publish_settings.is_validate_message_type() and message.message_type not in mq.accept_message_types:
                raise ValueError(
                    "Current message type does not match with the accept message types," +
                    f" topic={message.topic}, actualMessageType={message.message_type}" +
                    f" acceptMessageType={','.join(mq.accept_message_types)}")

            sendMessageRequest = self.wrap_send_message_request(message, mq)
            topic_data = self.topic_route_cache["normal_topic"]
            endpoints = topic_data.message_queues[2].broker.endpoints
            try:
                invocation = await self.client_manager.send_message(endpoints, sendMessageRequest, self.client_config.request_timeout)
                sendReceipts = SendReceipt.process_send_message_response(mq, invocation)
            
                sendReceipt = sendReceipts[0]
                if attempt > 1:
                    logger.info(
                        f"Re-send message successfully, topic={message.topic}, messageId={sendReceipt.messageId}," +
                        f" maxAttempts={maxAttempts}, endpoints={endpoints}, clientId={self.client_id}")
            
                return sendReceipt
            except Exception as e:
                exception = e
                self.isolated[endpoints] = True
                if attempt >= maxAttempts:
                    logger.error(f"Failed to send message finally, run out of attempt times, " +
                                    f"topic={message.topic}, maxAttempt={maxAttempts}, attempt={attempt}, " +
                                    f"endpoints={endpoints}, messageId={message.message_id}, clientId={self.client_id}")
                    raise

                if message.message_type == "Transaction":
                    logger.info(f"Failed to send transaction message, run out of attempt times, " +
                                    f"topic={message.topic}, maxAttempt=1, attempt={attempt}, " +
                                    f"endpoints={endpoints}, messageId={message.message_id}, clientId={self.client_id}")
                    raise
            
                if not isinstance(exception, TooManyRequestsException):
                    logger.info(f"Failed to send message, topic={message.topic}, maxAttempts={maxAttempts}, " +
                                    f"attempt={attempt}, endpoints={endpoints}, messageId={message.message_id}," +
                                    f" clientId={self.client_id}")
                    continue

                nextAttempt = 1 + attempt
                delay = retry_policy.get_next_attempt_delay(nextAttempt)
                await asyncio.sleep(delay.total_seconds())


    # async def send_message(self, message):
    #     retry_policy = self.get_retry_policy()
    #     maxAttempts = retry_policy.GetMaxAttempts()
    #     for attempt in range(maxAttempts):
            
    #     req = SendMessageRequest()
    #     req.messages.extend([message])
    #     topic_data = self.topic_route_cache["normal_topic"]
    #     endpoints = topic_data.message_queues[2].broker.endpoints
    #     return await self.client_manager.send_message(endpoints, req, 10)

    def get_settings(self):
        return self.publish_settings

    def get_retry_policy(self):
        return self.publish_settings.GetRetryPolicy()

async def test():
    credentials = SessionCredentials("uU5kBDYnmBf1hVPl", "6TtqkYNNC677PWXX")
    credentials_provider = SessionCredentialsProvider(credentials)
    client_config = ClientConfig(
        endpoints=Endpoints("rmq-cn-jaj390gga04.cn-hangzhou.rmq.aliyuncs.com:8080"),
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
    await producer.start()
    result = await producer.send_message(msg)
    # print(result)


if __name__ == "__main__":
    asyncio.run(test())
