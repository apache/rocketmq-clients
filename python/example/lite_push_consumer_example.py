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

from rocketmq import (ClientConfiguration, ConsumeResult, Credentials,
                      LitePushConsumer, Message, MessageListener)


class LiteTopicTestMessageListener(MessageListener):

    def consume(self, message: Message) -> ConsumeResult:
        print(f"consume message, {message}.")
        return ConsumeResult.SUCCESS


if __name__ == '__main__':
    endpoints = "foobar.com:8080"
    credentials = Credentials()
    # if auth enable
    # credentials = Credentials("ak", "sk")
    config = ClientConfiguration(endpoints, credentials)
    # with namespace
    # config = ClientConfiguration(endpoints, credentials, "namespace")
    bind_topic = "topic"
    consumer_group = "consumer-group"
    # A LitePushConsumer can only be bound to one topic
    lite_push_consumer = LitePushConsumer(config, consumer_group, bind_topic, LiteTopicTestMessageListener())

    try:
        lite_push_consumer.startup()
        for i in range(0, 10):
            lite_push_consumer.subscribe_lite("lite-test-" + str(i))
        try:
            input("Please Enter to Stop the Application.\r\n")
        except Exception as e:
            print(f"{lite_push_consumer} raise exception: {e}")
            lite_push_consumer.shutdown()
            print(f"{lite_push_consumer} shutdown.")
    except Exception as e:
        print(f"{lite_push_consumer} startup raise exception: {e}")
        lite_push_consumer.shutdown()
        print(f"{lite_push_consumer} shutdown.")
