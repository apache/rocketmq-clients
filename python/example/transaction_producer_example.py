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
        print(f"do TestChecker check, topic:{message.topic}, message_id: {message.message_id}, commit message.")
        return TransactionResolution.COMMIT


if __name__ == '__main__':
    endpoints = "foobar.com:8080"
    credentials = Credentials()
    # if auth enable
    # credentials = Credentials("ak", "sk")
    config = ClientConfiguration(endpoints, credentials)
    # with namespace
    # config = ClientConfiguration(endpoints, credentials, "namespace")
    topic = "topic"
    check_from_server = True  # commit message from server check
    producer = Producer(config, (topic,), checker=TestChecker())

    try:
        producer.startup()
    except Terminate as e:
        print(f"{producer} startup raise exception: {e}")

    try:
        transaction = producer.begin_transaction()
        msg = Message()
        # topic for the current message
        msg.topic = topic
        msg.body = "hello, rocketmq.".encode('utf-8')
        # secondary classifier of message besides topic
        msg.tag = "rocketmq-send-transaction-message"
        # key(s) of the message, another way to mark message besides message id
        msg.keys = "send_transaction"
        # user property for the message
        msg.add_property("send", "transaction")
        res = producer.send(msg, transaction)
        print(f"{producer} send message success. {res}")
        if check_from_server:
            # wait for server check in TransactionChecker's check
            input("Please Enter to Stop the Application.\r\n")
            producer.shutdown()
            print(f"{producer} shutdown.")
        else:
            # direct commit or rollback
            transaction.commit()
            print(f"{producer} commit message:{transaction.message_id}")
            # transaction.rollback()
            # print(f"{producer} rollback message:{transaction.message_id}")
            producer.shutdown()
            print(f"{producer} shutdown.")
    except Terminate as e:
        print(f"transaction producer{producer} example raise exception: {e}")
        producer.shutdown()
        print(f"{producer} shutdown.")
