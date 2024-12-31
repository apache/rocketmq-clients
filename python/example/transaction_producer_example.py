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

from rocketmq import (ClientConfiguration, Credentials, Message, Producer,
                      TransactionChecker, TransactionResolution)


class TestChecker(TransactionChecker):

    def check(self, message: Message) -> TransactionResolution:
        print(f"do TestChecker check. message_id: {message.message_id}, commit message.")
        return TransactionResolution.COMMIT


if __name__ == '__main__':
    endpoints = "foobar.com:8080"
    credentials = Credentials()
    # if auth enable
    # credentials = Credentials("ak", "sk")
    config = ClientConfiguration(endpoints, credentials)
    topic = "topic"
    producer = Producer(config, (topic,))

    try:
        producer.startup()
    except Exception as e:
        print(f"{producer.__str__()} startup raise exception: {e}")

    try:
        transaction = producer.begin_transaction()
        msg = Message()
        msg.topic = topic
        msg.body = "hello, rocketmq.".encode('utf-8')
        msg.tag = "rocketmq-send-transaction-message"
        msg.keys = "send_transaction"
        msg.add_property("send", "transaction")
        res = producer.send(msg, transaction)
        print(f"transaction producer{producer.__str__()} send half message success. {res}")
    except Exception as e:
        print(f"transaction producer{producer.__str__()} example raise exception: {e}")
