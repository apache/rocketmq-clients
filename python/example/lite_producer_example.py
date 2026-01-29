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

if __name__ == '__main__':
    endpoints = "foobar.com:8080"
    credentials = Credentials()

    # if auth enable
    # credentials = Credentials("ak", "sk")
    config = ClientConfiguration(endpoints, credentials)
    # with namespace
    # config = ClientConfiguration(endpoints, credentials, "namespace")
    topic = "parent-topic"
    producer = Producer(config, (topic,))

    try:
        producer.startup()
        try:
            msg = Message()
            # parent topic for the current message
            msg.topic = topic
            msg.body = "hello, rocketmq.".encode('utf-8')
            for i in range(0, 10):
                # set lite_topic
                msg.lite_topic("lite-test-" + str(i))
                res = producer.send(msg)
                print(f"{producer} send message success. {res}")
            producer.shutdown()
            print(f"{producer} shutdown.")
        except Terminate as e:
            print(f"{producer} raise exception: {e}")
            producer.shutdown()
            print(f"{producer} shutdown.")
    except Terminate as e:
        print(f"{producer} startup raise exception: {e}")
        producer.shutdown()
        print(f"{producer} shutdown.")
