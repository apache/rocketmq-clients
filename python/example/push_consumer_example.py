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

from rocketmq import (ClientConfiguration, Credentials, FilterExpression,
                      Message)
from rocketmq.v5.consumer import PushConsumer
from rocketmq.v5.consumer.message_listener import (ConsumeResult,
                                                   MessageListener)


class TestMessageListener(MessageListener):

    def consume(self, message: Message) -> ConsumeResult:
        print(f"consume message, topic:{message.topic}, message_id: {message.message_id}.")
        return ConsumeResult.SUCCESS


if __name__ == '__main__':
    endpoints = "foobar.com:8080"
    credentials = Credentials()

    # if auth enable
    # credentials = Credentials("ak", "sk")
    config = ClientConfiguration(endpoints, credentials)
    # with namespace
    # config = ClientConfiguration(endpoints, credentials, "namespace")
    topic = "topic"
    consumer_group = "consumer-group"
    # in most case, you don't need to create too many consumers, singleton pattern is recommended
    # close the simple consumer when you don't need it anymore
    push_consumer = PushConsumer(config, consumer_group, TestMessageListener(), {topic: FilterExpression()})

    try:
        push_consumer.startup()
        try:
            input("Please Enter to Stop the Application.\r\n")
        except Exception as e:
            print(f"{push_consumer} raise exception: {e}")
            push_consumer.shutdown()
            print(f"{push_consumer} shutdown.")
    except Exception as e:
        print(f"{push_consumer} startup raise exception: {e}")
        push_consumer.shutdown()
        print(f"{push_consumer} shutdown.")

