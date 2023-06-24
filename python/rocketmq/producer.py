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
from typing import Set

from rocketmq.client import Client
from rocketmq.client_config import ClientConfig
from rocketmq.protocol.definition_pb2 import Message as ProtoMessage
from rocketmq.protocol.definition_pb2 import Resource, SystemProperties
from rocketmq.protocol.service_pb2 import SendMessageRequest
from rocketmq.publish_settings import PublishingSettings
from rocketmq.rpc_client import Endpoints
from rocketmq.session_credentials import (SessionCredentials,
                                          SessionCredentialsProvider)


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
        topic_data = self.topic_route_cache['normal_topic']
        endpoints = topic_data.message_queues[2].broker.endpoints
        return await self.client_manager.send_message(endpoints, req, 10)

    def get_settings(self):
        return self.publish_settings


async def test():
    creds = SessionCredentials("uU5kBDYnmBf1hVPl", "6TtqkYNNC677PWXX")
    creds_provider = SessionCredentialsProvider(creds)
    client_config = ClientConfig(
        endpoints=Endpoints(
            "rmq-cn-jaj390gga04.cn-hangzhou.rmq.aliyuncs.com:8080"
        ),
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
    sysperf.message_id = "this is"
    msg.system_properties.CopyFrom(sysperf)
    print(msg)
    await producer.start_up()
    result = await producer.send_message(msg)
    print(result)


if __name__ == "__main__":
    asyncio.run(test())
