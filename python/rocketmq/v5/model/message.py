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
from rocketmq.v5.util import MessageIdCodec, Misc


class Message:

    def __init__(self):
        self.__body = None
        self.__topic = None
        self.__namespace = None
        self.__message_id = None
        self.__tag = None
        self.__message_group = None
        self.__delivery_timestamp = None
        self.__keys = set()
        self.__properties = dict()
        self.__born_host = None
        self.__born_timestamp = None
        self.__delivery_attempt = None
        self.__receipt_handle = None
        self.__message_type = None

    def __str__(self) -> str:
        return f"topic:{self.__topic}, tag:{self.__tag}, messageGroup:{self.__message_group}, " \
               f"deliveryTimestamp:{self.__delivery_timestamp}, keys:{self.__keys}, properties:{self.__properties}"

    def fromProtobuf(self, message: definition_pb2.Message):  # noqa
        try:
            self.__message_body_check_sum(message)
            self.__topic = message.topic.name
            self.__namespace = message.topic.resource_namespace
            self.__message_id = MessageIdCodec.decode(message.system_properties.message_id)
            self.__body = self.__uncompress_body(message)
            self.__tag = message.system_properties.tag
            self.__message_group = message.system_properties.message_group
            self.__born_host = message.system_properties.born_host
            self.__born_timestamp = message.system_properties.born_timestamp.seconds
            self.__delivery_attempt = message.system_properties.delivery_attempt
            self.__delivery_timestamp = message.system_properties.delivery_timestamp
            self.__receipt_handle = message.system_properties.receipt_handle
            self.__message_type = message.system_properties.message_type

            if message.system_properties.keys is not None:
                self.__keys.update(message.system_properties.keys)
            if message.user_properties is not None:
                self.__properties.update(message.user_properties)
            return self
        except Exception as e:
            raise e

    """ private """

    @staticmethod
    def __message_body_check_sum(message):
        if message.system_properties.body_digest.type == DigestType.CRC32:
            crc32_sum = Misc.crc32_checksum(message.body)
            if message.system_properties.body_digest.checksum != crc32_sum:
                raise Exception(f"(body_check_sum exception, {message.digest.checksum} != crc32_sum {crc32_sum}")
        elif message.system_properties.body_digest.type == DigestType.MD5:
            md5_sum = Misc.md5_checksum(message.body)
            if message.system_properties.body_digest.checksum != md5_sum:
                raise Exception(f"(body_check_sum exception, {message.digest.checksum} != crc32_sum {md5_sum}")
        elif message.system_properties.body_digest.type == DigestType.SHA1:
            sha1_sum = Misc.sha1_checksum(message.body)
            if message.system_properties.body_digest.checksum != sha1_sum:
                raise Exception(f"(body_check_sum exception, {message.digest.checksum} != crc32_sum {sha1_sum}")
        else:
            raise Exception(f"unsupported message body digest algorithm, {message.system_properties.body_digest.type},"
                            f" {message.topic}, {message.system_properties.message_id}")

    @staticmethod
    def __uncompress_body(message):
        if message.system_properties.body_encoding == Encoding.GZIP:
            return Misc.uncompress_bytes_gzip(message.body)
        elif message.system_properties.body_encoding == Encoding.IDENTITY:
            return message.body
        else:
            raise Exception(
                f"unsupported message encoding algorithm, {message.system_properties.body_encoding}, {message.topic}, {message.system_properties.message_id}")

    """ property """

    @property
    def body(self):
        return self.__body

    @property
    def topic(self):
        return self.__topic

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

    @body.setter
    def body(self, body):
        if body is None or body.strip() == '':
            raise IllegalArgumentException("body should not be blank")
        self.__body = body

    @topic.setter
    def topic(self, topic):
        if Misc.is_valid_topic(topic):
            self.__topic = topic
        else:
            raise IllegalArgumentException(f"topic does not match the regex [regex={Misc.TOPIC_PATTERN}]")

    @message_id.setter
    def message_id(self, message_id):
        self.__message_id = message_id

    @tag.setter
    def tag(self, tag):
        if tag is None or tag.strip() == '':
            raise IllegalArgumentException("tag should not be blank")
        if "|" in tag:
            raise IllegalArgumentException("tag should not contain \"|\"")
        self.__tag = tag

    @message_group.setter
    def message_group(self, message_group):
        if self.__delivery_timestamp is not None:
            raise IllegalArgumentException("deliveryTimestamp and messageGroup should not be set at same time")
        if message_group is None or len(message_group) == 0:
            raise IllegalArgumentException("messageGroup should not be blank")
        self.__message_group = message_group

    @delivery_timestamp.setter
    def delivery_timestamp(self, delivery_timestamp):
        if self.__message_group is not None:
            raise IllegalArgumentException("deliveryTimestamp and messageGroup should not be set at same time")
        self.__delivery_timestamp = delivery_timestamp

    @keys.setter
    def keys(self, *keys):
        for key in keys:
            if not key or key.strip() == '':
                raise IllegalArgumentException("key should not be blank")
        self.__keys.update(set(keys))

    @message_type.setter
    def message_type(self, message_type):
        self.__message_type = message_type

    def add_property(self, key, value):
        if key is None or key.strip() == '':
            raise IllegalArgumentException("key should not be blank")
        if value is None or value.strip() == '':
            raise IllegalArgumentException("value should not be blank")
        self.__properties[key] = value
