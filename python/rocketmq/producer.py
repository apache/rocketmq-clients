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

import rocketmq
from rocketmq.client import Client
from rocketmq.client_config import ClientConfig
from rocketmq.definition import TopicRouteData
from rocketmq.protocol.definition_pb2 import Message as ProtoMessage
from rocketmq.protocol.definition_pb2 import Resource, SystemProperties
from rocketmq.protocol.service_pb2 import SendMessageRequest
from rocketmq.publish_settings import PublishingSettings
from rocketmq.rpc_client import Endpoints
from rocketmq.message_id_codec import MessageIdCodec
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
        self.publish_settings = PublishingSettings(
            self.client_id, self.endpoints, None, 10, topics
        )

    async def start_up(self):
        await super().start_up()

    async def send_message(self, message):
        req = SendMessageRequest()
        req.messages.extend([message])
        topic_data = self.topic_route_cache["normal_topic"]
        endpoints = topic_data.message_queues[2].broker.endpoints
        return await self.client_manager.send_message(endpoints, req, 10)

    def get_settings(self):
        return self.publish_settings


async def test():
    creds = SessionCredentials("username", "password")
    creds_provider = SessionCredentialsProvider(creds)
    client_config = ClientConfig(
        endpoints=Endpoints("rmq-cn-jaj390gga04.cn-hangzhou.rmq.aliyuncs.com:8080"),
        session_credentials_provider=creds_provider,
        ssl_enabled=True,
    )
    producer = Producer(client_config, topics={"normal_topic"})
    topic = Resource()
    topic.name = "normal_topic"
    msg = ProtoMessage()
    msg.topic.CopyFrom(topic)
    msg.body = b"My Message Body"
    sysperf = SystemProperties()
    sysperf.message_id = MessageIdCodec.next_message_id()
    msg.system_properties.CopyFrom(sysperf)
    print(msg)
    await producer.start_up()
    result = await producer.send_message(msg)
    print(result)


if __name__ == "__main__":
    asyncio.run(test())
