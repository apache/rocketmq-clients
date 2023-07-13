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

import socket

from definition import Encoding, EncodingHelper, MessageType, MessageTypeHelper
from google.protobuf.timestamp_pb2 import Timestamp
from message import Message
from message_id_codec import MessageIdCodec
from protocol.definition_pb2 import Message as ProtoMessage
from protocol.definition_pb2 import Resource, SystemProperties
from rocketmq.log import logger


class PublishingMessage(Message):
    def __init__(self, message, publishing_settings, tx_enabled=False):
        self.message = message
        self.publishing_settings = publishing_settings
        self.tx_enabled = tx_enabled
        self.message_type = None

        max_body_size_bytes = publishing_settings.get_max_body_size_bytes()
        if len(message.body) > max_body_size_bytes:
            raise IOError(f"Message body size exceed the threshold, max size={max_body_size_bytes} bytes")

        self.message_id = MessageIdCodec.next_message_id()

        if not message.message_group and not message.delivery_timestamp and not tx_enabled:
            self.message_type = MessageType.NORMAL
            return

        if message.message_group and not tx_enabled:
            self.message_type = MessageType.FIFO
            return

        if message.delivery_timestamp and not tx_enabled:
            self.message_type = MessageType.DELAY
            return

        if message.message_group or message.delivery_timestamp or not tx_enabled:
            pass

        self.message_type = MessageType.TRANSACTION
        logger.debug(self.message_type)

    def to_protobuf(self, queue_id):
        system_properties = SystemProperties(
            keys=self.message.keys,
            message_id=self.message_id,
            # born_timestamp=Timestamp.FromDatetime(dt=datetime.datetime.utcnow()),
            born_host=socket.gethostname(),
            body_encoding=EncodingHelper.to_protobuf(Encoding.IDENTITY),
            queue_id=queue_id,
            message_type=MessageTypeHelper.to_protobuf(self.message_type)
        )
        if self.message.tag:
            system_properties.tag = self.message.tag

        if self.message.delivery_timestamp:
            timestamp = Timestamp()
            timestamp.FromDatetime(self.message.delivery_timestamp)
            system_properties.delivery_timestamp.CopyFrom(timestamp)

        if self.message.message_group:
            system_properties.message_group = self.message.message_group

        topic_resource = Resource(name=self.message.topic)

        return ProtoMessage(
            topic=topic_resource,
            body=self.message.body,
            system_properties=system_properties,
            user_properties=self.message.properties
        )
