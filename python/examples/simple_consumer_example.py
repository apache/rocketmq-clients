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

from rocketmq.client_config import ClientConfig
from rocketmq.filter_expression import FilterExpression
from rocketmq.log import logger
from rocketmq.protocol.definition_pb2 import Resource
from rocketmq.rpc_client import Endpoints
from rocketmq.session_credentials import (SessionCredentials,
                                          SessionCredentialsProvider)
from rocketmq.simple_consumer import SimpleConsumer


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

if __name__ == "__main__":
    asyncio.run(test())
