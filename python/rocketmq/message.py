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


import binascii
import gzip
import hashlib
from typing import Dict, List

from rocketmq.definition import MessageQueue
from rocketmq.protocol.definition_pb2 import DigestType as ProtoDigestType
from rocketmq.protocol.definition_pb2 import Encoding as ProtoEncoding


class Message:
    def __init__(
        self,
        topic: str,
        body: bytes,
        properties: map = None,
        tag: str = None,
        keys: str = None,
        message_group: str = None,
        delivery_timestamp: int = None,
    ):
        if properties is None:
            properties = {}
        self.__topic = topic
        self.__body = body
        self.__properties = properties
        self.__tag = tag
        self.__keys = keys
        self.__message_group = message_group
        self.__delivery_timestamp = delivery_timestamp

    @property
    def topic(self):
        return self.__topic

    @property
    def body(self):
        return self.__body

    @property
    def properties(self):
        return self.__properties

    @property
    def tag(self):
        return self.__tag

    @property
    def keys(self):
        return self.__keys

    @property
    def message_group(self):
        return self.__message_group

    @property
    def delivery_timestamp(self):
        return self.__delivery_timestamp


class MessageView:
    def __init__(
        self,
        message_id: str,
        topic: str,
        body: bytes,
        tag: str,
        message_group: str,
        delivery_timestamp: int,
        keys: List[str],
        properties: Dict[str, str],
        born_host: str,
        born_time: int,
        delivery_attempt: int,
        message_queue: MessageQueue,
        receipt_handle: str,
        offset: int,
        corrupted: bool
    ):
        self.__message_id = message_id
        self.__topic = topic
        self.__body = body
        self.__properties = properties
        self.__tag = tag
        self.__keys = keys
        self.__message_group = message_group
        self.__delivery_timestamp = delivery_timestamp
        self.__born_host = born_host
        self.__delivery_attempt = delivery_attempt
        self.__receipt_handle = receipt_handle
        self.__born_time = born_time
        self.__message_queue = message_queue
        self.__offset = offset
        self.__corrupted = corrupted

    @property
    def message_queue(self):
        return self.__message_queue

    @property
    def receipt_handle(self):
        return self.__receipt_handle

    @property
    def topic(self):
        return self.__topic

    @property
    def body(self):
        return self.__body

    @property
    def message_id(self):
        return self.__message_id

    @property
    def born_host(self):
        return self.__born_host

    @property
    def keys(self):
        return self.__keys

    @property
    def properties(self):
        return self.__properties

    @property
    def tag(self):
        return self.__tag

    @property
    def message_group(self):
        return self.__message_group

    @property
    def delivery_timestamp(self):
        return self.__delivery_timestamp

    @classmethod
    def from_protobuf(cls, message, message_queue=None):
        topic = message.topic.name
        system_properties = message.system_properties
        message_id = system_properties.message_id
        body_digest = system_properties.body_digest
        check_sum = body_digest.checksum
        raw = message.body
        corrupted = False
        digest_type = body_digest.type

        # Digest Type check
        if digest_type == ProtoDigestType.CRC32:
            expected_check_sum = format(binascii.crc32(raw) & 0xFFFFFFFF, '08X')
            if not expected_check_sum == check_sum:
                corrupted = True
        elif digest_type == ProtoDigestType.MD5:
            expected_check_sum = hashlib.md5(raw).hexdigest()
            if not expected_check_sum == check_sum:
                corrupted = True
        elif digest_type == ProtoDigestType.SHA1:
            expected_check_sum = hashlib.sha1(raw).hexdigest()
            if not expected_check_sum == check_sum:
                corrupted = True
        elif digest_type in [ProtoDigestType.unspecified, None]:
            print(f"Unsupported message body digest algorithm, digestType={digest_type}, topic={topic}, messageId={message_id}")

        # Body Encoding check
        body_encoding = system_properties.body_encoding
        body = raw
        if body_encoding == ProtoEncoding.GZIP:
            body = gzip.decompress(message.body)
        elif body_encoding in [ProtoEncoding.IDENTITY, None]:
            pass
        else:
            print(f"Unsupported message encoding algorithm, topic={topic}, messageId={message_id}, bodyEncoding={body_encoding}")

        tag = system_properties.tag
        message_group = system_properties.message_group
        delivery_time = system_properties.delivery_timestamp
        keys = list(system_properties.keys)

        born_host = system_properties.born_host
        born_time = system_properties.born_timestamp
        delivery_attempt = system_properties.delivery_attempt
        queue_offset = system_properties.queue_offset
        properties = {key: value for key, value in message.user_properties.items()}
        receipt_handle = system_properties.receipt_handle

        return cls(message_id, topic, body, tag, message_group, delivery_time, keys, properties, born_host,
                   born_time, delivery_attempt, message_queue, receipt_handle, queue_offset, corrupted)
