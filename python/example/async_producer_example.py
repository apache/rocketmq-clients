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
        # don't write time-consuming code in the callback. if needed, use other thread
        res = result_future.result()
        print(f"send message success, {res}")
    except Exception as exception:
        print(f"send message failed, raise exception: {exception}")


if __name__ == '__main__':
    endpoints = "foobar.com:8080"
    credentials = Credentials()
    # if auth enable
    # credentials = Credentials("ak", "sk")
    config = ClientConfiguration(endpoints, credentials)
    # with namespace
    # config = ClientConfiguration(endpoints, credentials, "namespace")
    topic = "topic"
    producer = Producer(config, (topic,))

    try:
        producer.startup()
        try:
            for i in range(10):
                msg = Message()
                # topic for the current message
                msg.topic = topic
                msg.body = "hello, rocketmq.".encode('utf-8')
                # secondary classifier of message besides topic
                msg.tag = "tag"
                # key(s) of the message, another way to mark message besides message id
                msg.keys = "keys"
                # user property for the message
                msg.add_property("send", "async")
                send_result_future = producer.send_async(msg)
                send_result_future.add_done_callback(handle_send_result)
        except Exception as e:
            print(f"{producer} raise exception: {e}")
    except Exception as e:
        print(f"{producer} startup raise exception: {e}")

    input("Please Enter to Stop the Application.")
    producer.shutdown()
    print(f"{producer} shutdown.")
