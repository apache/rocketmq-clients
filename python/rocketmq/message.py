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


from rocketmq.message_id import MessageId


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
        message_id: MessageId,
        topic: str,
        body: bytes,
        properties: map,
        tag: str,
        keys: str,
        message_group: str,
        delivery_timestamp: int,
        born_host: str,
        delivery_attempt: int,
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

    @property
    def topic(self):
        return self.__topic

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
    def keys(self):
        return self.__keys

    @property
    def message_group(self):
        return self.__message_group

    @property
    def delivery_timestamp(self):
        return self.__delivery_timestamp
