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

from rocketmq.grpc_protocol import DigestType, Encoding, definition_pb2
from rocketmq.v5.exception import IllegalArgumentException
from rocketmq.v5.log import logger
from rocketmq.v5.util import Misc


class Message:

    def __init__(self):
        self.__body = None
        self.__topic = None
        self.__lite_topic = None
        self.__namespace = None
        self.__message_id = None
        self.__tag = None
        self.__message_group = None
        self.__delivery_timestamp = None
        self.__transport_delivery_timestamp = None
        self.__decode_message_timestamp = None
        self.__keys = set()
        self.__properties = dict()
        self.__born_host = None
        self.__born_timestamp = None
        self.__delivery_attempt = None
        self.__receipt_handle = None
        self.__message_type = None
        self.__endpoints = None
        self.__corrupted = False

    def __str__(self) -> str:
        return (
            f"topic:{self.__topic}, tag:{self.__tag}, messageGroup:{self.__message_group}, "
            f"deliveryTimestamp:{self.__delivery_timestamp}, keys:{self.__keys}, properties:{self.__properties}"
        )

    def fromProtobuf(self, message: definition_pb2.Message):  # noqa
        try:
            self.__message_body_check_sum(message)
            self.__topic = message.topic.name
            if message.system_properties.lite_topic:
                self.__lite_topic = message.system_properties.lite_topic
            self.__namespace = message.topic.resource_namespace
            self.__message_id = message.system_properties.message_id
            self.__body = self.__uncompress_body(message)
            self.__tag = message.system_properties.tag
            if message.system_properties.message_group:
                self.__message_group = message.system_properties.message_group
            self.__born_host = message.system_properties.born_host
            self.__born_timestamp = Misc.to_mills(message.system_properties.born_timestamp)
            self.__delivery_attempt = message.system_properties.delivery_attempt
            if message.system_properties.delivery_timestamp:
                self.__delivery_timestamp = Misc.to_mills(message.system_properties.delivery_timestamp)
            self.__decode_message_timestamp = Misc.current_mills()
            self.__receipt_handle = message.system_properties.receipt_handle
            self.__message_type = message.system_properties.message_type
            if not message.system_properties.keys:
                self.__keys.update(message.system_properties.keys)
            if not message.user_properties:
                self.__properties.update(message.user_properties)
            return self
        except Exception as e:
            raise e

    """ private """

    def __message_body_check_sum(self, message):
        try:
            if message.system_properties.body_digest.type == DigestType.CRC32:
                crc32_sum = Misc.crc32_checksum(message.body)
                if message.system_properties.body_digest.checksum != crc32_sum:
                    self.__corrupted = True
                    logger.error(
                        f"(body_check_sum exception, topic: {message.topic.name}, message_id: {message.system_properties.message_id} {message.digest.checksum} != crc32_sum {crc32_sum}"
                    )
            elif message.system_properties.body_digest.type == DigestType.MD5:
                md5_sum = Misc.md5_checksum(message.body)
                if message.system_properties.body_digest.checksum != md5_sum:
                    self.__corrupted = True
                    logger.error(
                        f"(body_check_sum exception, topic: {message.topic.name}, message_id: {message.system_properties.message_id} {message.digest.checksum} != md5_checksum {md5_sum}"
                    )
            elif message.system_properties.body_digest.type == DigestType.SHA1:
                sha1_sum = Misc.sha1_checksum(message.body)
                if message.system_properties.body_digest.checksum != sha1_sum:
                    self.__corrupted = True
                    logger.error(
                        f"(body_check_sum exception, topic: {message.topic.name}, message_id: {message.system_properties.message_id} {message.digest.checksum} != sha1_checksum {sha1_sum}"
                    )
            else:
                logger.error(
                    f"unsupported message body digest algorithm, {message.system_properties.body_digest.type},"
                    f" {message.topic.name}, {message.system_properties.message_id}"
                )
        except Exception as e:
            self.__corrupted = True
            logger.error(
                f"(body_check_sum exception, topic: {message.topic.name}, message_id: {message.system_properties.message_id}, {e}"
            )

    @staticmethod
    def __uncompress_body(message):
        if message.system_properties.body_encoding == Encoding.GZIP:
            return Misc.uncompress_bytes_gzip(message.body)
        elif message.system_properties.body_encoding == Encoding.IDENTITY:
            return message.body
        else:
            raise Exception(
                f"unsupported message encoding algorithm, {message.system_properties.body_encoding}, {message.topic}, {message.system_properties.message_id}"
            )

    """ property """

    @property
    def body(self):
        return self.__body

    @property
    def topic(self):
        return self.__topic

    @property
    def lite_topic(self):
        return self.__lite_topic

    @property
    def namespace(self):
        return self.__namespace

    @property
    def message_id(self):
        return self.__message_id

    @property
    def tag(self):
        return self.__tag

    @property
    def message_group(self):
        return self.__message_group

    @property
    def delivery_timestamp(self):
        return self.__delivery_timestamp

    @property
    def transport_delivery_timestamp(self):
        return self.__transport_delivery_timestamp

    @property
    def decode_message_timestamp(self):
        return self.__decode_message_timestamp

    @property
    def keys(self):
        return self.__keys

    @property
    def properties(self):
        return self.__properties

    @property
    def born_host(self):
        return self.__born_host

    @property
    def born_timestamp(self):
        return self.__born_timestamp

    @property
    def delivery_attempt(self):
        return self.__delivery_attempt

    @property
    def receipt_handle(self):
        return self.__receipt_handle

    @property
    def message_type(self):
        return self.__message_type

    @property
    def endpoints(self):
        return self.__endpoints

    @property
    def corrupted(self):
        return self.__corrupted

    @body.setter
    def body(self, body: bytes):
        if not body:
            raise IllegalArgumentException("body should not be blank.")
        self.__body = body

    @topic.setter
    def topic(self, topic: str):
        if not topic or not topic.strip():
            raise IllegalArgumentException("topic has not been set yet.")
        if Misc.is_valid_topic(topic):
            self.__topic = topic
        else:
            raise IllegalArgumentException(f"topic does not match the regex [regex={Misc.TOPIC_PATTERN}].")

    @lite_topic.setter
    def lite_topic(self, lite_topic: str):
        if not lite_topic or not lite_topic.strip():
            raise IllegalArgumentException("lite_topic has not been set yet.")
        if self.__message_group:
            raise IllegalArgumentException("lite_topic and message_group should not be set at same time.")
        if self.__delivery_timestamp:
            raise IllegalArgumentException("lite_topic and delivery_timestamp should not be set at same time.")
        self.__lite_topic = lite_topic

    @message_id.setter
    def message_id(self, message_id):
        self.__message_id = message_id

    @tag.setter
    def tag(self, tag: str):
        if not tag or not tag.strip():
            raise IllegalArgumentException("tag should not be blank.")
        if "|" in tag:
            raise IllegalArgumentException('tag should not contain "|".')
        self.__tag = tag

    @message_group.setter
    def message_group(self, message_group: str):
        if not message_group or not message_group.strip():
            raise IllegalArgumentException("message_group should not be blank.")
        if self.__delivery_timestamp:
            raise IllegalArgumentException("delivery_timestamp and message_group should not be set at same time.")
        if self.__lite_topic:
            raise IllegalArgumentException("lite_topic and message_group should not be set at same time.")
        self.__message_group = message_group

    @delivery_timestamp.setter
    def delivery_timestamp(self, delivery_timestamp: int):
        if self.__message_group:
            raise IllegalArgumentException("delivery_timestamp and message_group should not be set at same time.")
        if self.__lite_topic:
            raise IllegalArgumentException("lite_topic and delivery_timestamp should not be set at same time.")
        self.__delivery_timestamp = delivery_timestamp

    @transport_delivery_timestamp.setter
    def transport_delivery_timestamp(self, transport_delivery_timestamp):
        self.__transport_delivery_timestamp = transport_delivery_timestamp

    @keys.setter
    def keys(self, *keys: str):
        for key in keys:
            if not key or not key.strip():
                raise IllegalArgumentException("key should not be blank.")
        self.__keys.update(set(keys))

    @receipt_handle.setter
    def receipt_handle(self, receipt_handle):
        self.__receipt_handle = receipt_handle

    @message_type.setter
    def message_type(self, message_type):
        self.__message_type = message_type

    @endpoints.setter
    def endpoints(self, endpoints):
        self.__endpoints = endpoints

    def add_property(self, key: str, value: str):
        if not key or not key.strip():
            raise IllegalArgumentException("key should not be blank.")
        if not value or not value.strip():
            raise IllegalArgumentException("value should not be blank.")
        self.__properties[key] = value

    @staticmethod
    def message_type_desc(message_type):
        if message_type == 1:
            return "NORMAL"
        elif message_type == 2:
            return "FIFO"
        elif message_type == 3:
            return "DELAY"
        elif message_type == 4:
            return "TRANSACTION"
        elif message_type == 5:
            return "LITE"
        else:
            return "MESSAGE_TYPE_UNSPECIFIED"
