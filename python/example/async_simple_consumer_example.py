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

import functools
import time

from rocketmq import ClientConfiguration, Credentials, SimpleConsumer


def receive_callback(receive_result_future, consumer):
    messages = receive_result_future.result()
    for msg in messages:
        print(f"{consumer.__str__()} receive {len(messages)} messages in callback.")
        try:
            consumer.ack(msg)
            print(f"receive and ack message:{msg.message_id} in callback.")
        except Exception as exception:
            print(f"receive message callback raise exception: {exception}")


if __name__ == '__main__':
    endpoints = "endpoints"
    credentials = Credentials("ak", "sk")
    config = ClientConfiguration(endpoints, credentials)
    topic = "topic"
    try:
        simple_consumer = SimpleConsumer(config, "consumer_group")
        simple_consumer.startup()
        simple_consumer.subscribe(topic)
        while True:
            time.sleep(5)
            future = simple_consumer.receive_async(32, 15)
            future.add_done_callback(functools.partial(receive_callback, consumer=simple_consumer))
    except Exception as e:
        print(f"simple consumer example raise exception: {e}")
