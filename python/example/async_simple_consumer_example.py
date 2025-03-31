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
from concurrent.futures.thread import ThreadPoolExecutor

from rocketmq import ClientConfiguration, Credentials, SimpleConsumer

consume_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="consume-message")


def consume_message(consumer, message):
    try:
        consumer.ack(message)
        print(f"ack message:{message.message_id}.")
    except Exception as exception:
        print(f"consume message raise exception: {exception}")


def receive_callback(receive_result_future, consumer):
    messages = receive_result_future.result()
    print(f"{consumer.__str__()} receive {len(messages)} messages.")
    for msg in messages:
        try:
            # consume message in other thread, don't block the async receive thread
            consume_executor.submit(consume_message, consumer=consumer, message=msg)
        except Exception as exception:
            print(f"receive message raise exception: {exception}")


if __name__ == '__main__':
    endpoints = "foobar.com:8080"
    credentials = Credentials()
    # if auth enable
    # credentials = Credentials("ak", "sk")
    config = ClientConfiguration(endpoints, credentials)
    # with namespace
    # config = ClientConfiguration(endpoints, credentials, "namespace")
    topic = "topic"
    # in most case, you don't need to create too many consumers, singleton pattern is recommended
    # close the simple consumer when you don't need it anymore
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
                    # max message num for each long polling and message invisible duration after it is received
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
