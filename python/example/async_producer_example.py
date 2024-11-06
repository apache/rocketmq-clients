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

from rocketmq import ClientConfiguration, Credentials, Message, Producer


def handle_send_result(result_future):
    try:
        res = result_future.result()
        print(f"async send message success, {res}")
    except Exception as exception:
        print(f"async send message failed, raise exception: {exception}")


if __name__ == '__main__':
    endpoints = "endpoints"
    credentials = Credentials("ak", "sk")
    config = ClientConfiguration(endpoints, credentials)
    topic = "topic"
    try:
        producer = Producer(config, (topic,))
        producer.startup()
        msg = Message()
        msg.topic = topic
        msg.body = "hello, rocketmq.".encode('utf-8')
        msg.tag = "rocketmq-send-message"
        msg.keys = "send_async"
        msg.add_property("send", "async")
        send_result_future = producer.send_async(msg)
        send_result_future.add_done_callback(handle_send_result)
    except Exception as e:
        print(f"async producer example raise exception: {e}")
