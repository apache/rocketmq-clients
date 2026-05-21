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

from rocketmq.grpc_protocol import (AddressScheme, Broker, Code, Endpoints,
                                    MessageType, Permission, Resource, Status,
                                    definition_pb2)
from rocketmq.grpc_protocol.service_pb2 import ReceiveMessageResponse  # noqa
from rocketmq.grpc_protocol.service_pb2 import SendMessageResponse  # noqa
from rocketmq.v5.client import ClientConfiguration, Credentials
from rocketmq.v5.model import Message, MessageQueue, SendReceipt
from rocketmq.v5.util import ClientId, MessageIdCodec, Misc


class TestBase:
    FAKE_TRANSACTION_ID = "foo-bar-transaction-id"
    FAKE_NAMESPACE = "foo-bar-namespace"
    FAKE_AK = "foo-bar-ak"
    FAKE_SK = "foo-bar-sk"
    FAKE_CLIENT_ID = ClientId()
    FAKE_TOPIC_0 = "foo-bar-topic-0"
    FAKE_TOPIC_1 = "foo-bar-topic-1"
    FAKE_MESSAGE_BODY = "foobar".encode("utf-8")
    FAKE_TAG_0 = "foo-bar-tag-0"
    FAKE_BROKER_NAME_0 = "foo-bar-broker-name-0"
    FAKE_BROKER_NAME_1 = "foo-bar-broker-name-1"
    FAKE_RECEIPT_HANDLE_0 = "foo-bar-handle-0"
    FAKE_RECEIPT_HANDLE_1 = "foo-bar-handle-1"
    FAKE_ENDPOINTS = "127.0.0.1:9876"
    FAKE_HOST_0 = "127.0.0.1"
    FAKE_PORT_0 = 8080
    FAKE_HOST_1 = "127.0.0.2"
    FAKE_PORT_1 = 8081
    FAKE_CONSUMER_GROUP_0 = "foo-bar-group-0"

    @staticmethod
    def fake_client_config():
        credentials = Credentials(TestBase.FAKE_AK, TestBase.FAKE_SK)
        config = ClientConfiguration(
            TestBase.FAKE_ENDPOINTS, credentials, TestBase.FAKE_NAMESPACE
        )
        return config

    @staticmethod
    def fake_topic_resource(topic):
        fake_resource = Resource()
        fake_resource.name = topic
        fake_resource.resource_namespace = TestBase.FAKE_NAMESPACE
        return fake_resource

    @staticmethod
    def fake_send_message(topic):
        msg = Message()
        msg.topic = topic
        msg.body = TestBase.FAKE_MESSAGE_BODY
        return msg

    @staticmethod
    def fake_Receive_message(topic):
        msg = definition_pb2.Message()  # noqa
        msg.topic.name = TestBase.FAKE_TOPIC_0
        msg.topic.resource_namespace = TestBase.FAKE_NAMESPACE
        msg.system_properties.message_id = MessageIdCodec().next_message_id()
        msg.body = TestBase.FAKE_MESSAGE_BODY
        msg.system_properties.born_host = TestBase.FAKE_HOST_0
        msg.system_properties.born_timestamp.seconds = Misc.current_mills()
        msg.system_properties.delivery_timestamp.seconds = (
            msg.system_properties.born_timestamp.seconds - 10
        )
        msg.system_properties.message_type = 1
        msg.system_properties.body_encoding = 1
        return msg

    @staticmethod
    def fake_broker():
        fake_broker = Broker()
        endpoints = Endpoints()
        endpoints.scheme = AddressScheme.IPv4
        address = endpoints.addresses.add()
        address.host = TestBase.FAKE_HOST_0
        address.port = TestBase.FAKE_PORT_0
        fake_broker.name = TestBase.FAKE_BROKER_NAME_0
        fake_broker.id = 0
        fake_broker.endpoints.CopyFrom(endpoints)
        return fake_broker

    @staticmethod
    def fake_queue(topic):
        fake_resource = TestBase.fake_topic_resource(topic)
        fake_broker = TestBase.fake_broker()
        fake_queue = definition_pb2.MessageQueue()  # noqa
        fake_queue.topic.CopyFrom(fake_resource)
        fake_queue.id = 0
        fake_queue.broker.CopyFrom(fake_broker)
        fake_queue.permission = Permission.READ_WRITE
        fake_queue.accept_message_types.extend(
            (
                MessageType.NORMAL,
                MessageType.FIFO,
                MessageType.DELAY,
                MessageType.TRANSACTION,
            )
        )
        return MessageQueue(fake_queue)

    @staticmethod
    def fake_message_queue(topic):
        fake_message_queue = MessageQueue(TestBase.fake_queue(topic))
        return fake_message_queue

    @staticmethod
    def fake_ok_status():
        status = Status()
        status.code = Code.OK
        status.message = "OK"
        return status

    @staticmethod
    def fake_send_success_response():
        fake_response = SendMessageResponse()
        status = TestBase.fake_ok_status()
        fake_response.status.CopyFrom(status)
        entry = fake_response.entries.add()
        entry.status.CopyFrom(status)
        entry.message_id = MessageIdCodec().next_message_id()
        entry.transaction_id = entry.message_id
        entry.offset = 999
        return fake_response

    @staticmethod
    def fake_send_receipt(topic):
        fake_response = TestBase.fake_send_success_response()
        fake_message_queue = TestBase.fake_queue(topic)
        fake_entry = fake_response.entries[0]
        return SendReceipt(
            fake_entry.message_id,
            fake_entry.transaction_id,
            fake_message_queue,
            fake_entry.offset,
        )

    @staticmethod
    def fake_receive_receipt():
        res_status = ReceiveMessageResponse()
        res_status.status.CopyFrom(TestBase.fake_ok_status())
        res_msg = ReceiveMessageResponse()
        msg = TestBase.fake_Receive_message(TestBase.FAKE_TOPIC_0)
        res_msg.message.CopyFrom(msg)
        return [res_status, res_msg]
