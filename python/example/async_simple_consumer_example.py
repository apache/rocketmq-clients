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
    print(f"{consumer.__str__()} receive {len(messages)} messages.")
    for msg in messages:
        try:
            consumer.ack(msg)
            print(f"ack message:{msg.message_id}.")
        except Exception as exception:
            print(f"receive message raise exception: {exception}")


if __name__ == '__main__':
    endpoints = "foobar.com:8080"
    credentials = Credentials()
    # if auth enable
    # credentials = Credentials("ak", "sk")
    config = ClientConfiguration(endpoints, credentials)
    topic = "topic"

    simple_consumer = SimpleConsumer(config, "consumer-group")
    try:
        simple_consumer.startup()
        try:
            simple_consumer.subscribe(topic)
            # use tag filter
            # simple_consumer.subscribe(topic, FilterExpression("tag"))
            while True:
                try:
                    time.sleep(1)
                    future = simple_consumer.receive_async(32, 15)
                    future.add_done_callback(functools.partial(receive_callback, consumer=simple_consumer))
                except Exception as e:
                    print(f"{simple_consumer.__str__()} receive topic:{topic} raise exception: {e}")
        except Exception as e:
            print(f"{simple_consumer.__str__()} subscribe topic:{topic} raise exception: {e}")
            simple_consumer.shutdown()
    except Exception as e:
        print(f"{simple_consumer.__str__()} startup raise exception: {e}")
        simple_consumer.shutdown()
