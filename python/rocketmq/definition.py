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

from enum import Enum
from typing import List

from protocol.definition_pb2 import Broker as ProtoBroker
from protocol.definition_pb2 import Encoding as ProtoEncoding
from protocol.definition_pb2 import MessageQueue as ProtoMessageQueue
from protocol.definition_pb2 import MessageType as ProtoMessageType
from protocol.definition_pb2 import Permission as ProtoPermission
from protocol.definition_pb2 import Resource as ProtoResource
from rocketmq.protocol import definition_pb2
from rocketmq.rpc_client import Endpoints


class Encoding(Enum):
    """Enumeration of supported encoding types."""
    IDENTITY = 0
    GZIP = 1


class EncodingHelper:
    """Helper class for converting encoding types to protobuf."""

    @staticmethod
    def to_protobuf(mq_encoding):
        """Convert encoding type to protobuf.

        :param mq_encoding: The encoding to be converted.
        :return: The corresponding protobuf encoding.
        """
        if mq_encoding == Encoding.IDENTITY:
            return ProtoEncoding.IDENTITY
        elif mq_encoding == Encoding.GZIP:
            return ProtoEncoding.GZIP


class Broker:
    """Represent a broker entity."""

    def __init__(self, broker):
        self.name = broker.name
        self.id = broker.id
        self.endpoints = Endpoints(broker.endpoints)

    def to_protobuf(self):
        """Convert the broker to its protobuf representation.

        :return: The protobuf representation of the broker.
        """
        return ProtoBroker(
            name=self.name, id=self.id, endpoints=self.endpoints.to_protobuf()
        )


class Resource:
    """Represent a resource entity."""

    def __init__(self, name=None, resource=None):
        """Initialize a resource.

        :param name: The name of the resource.
        :param resource: The resource object.
        """
        if resource is not None:
            self.namespace = resource.resource_namespace
            self.name = resource.name
        else:
            self.namespace = ""
            self.name = name

    def to_protobuf(self):
        """Convert the resource to its protobuf representation.

        :return: The protobuf representation of the resource.
        """
        resource = ProtoResource()
        resource.name = self.name
        resource.resource_namespace = self.namespace
        return resource

    def __str__(self):
        return f"{self.namespace}.{self.name}" if self.namespace else self.name


class Permission(Enum):
    """Enumeration of supported permission types."""
    NONE = 0
    READ = 1
    WRITE = 2
    READ_WRITE = 3


class PermissionHelper:
    """Helper class for converting permission types to protobuf and vice versa."""

    @staticmethod
    def from_protobuf(permission):
        """Convert protobuf permission to Permission enum.

        :param permission: The protobuf permission to be converted.
        :return: The corresponding Permission enum.
        """
        if permission == ProtoPermission.READ:
            return Permission.READ
        elif permission == ProtoPermission.WRITE:
            return Permission.WRITE
        elif permission == ProtoPermission.READ_WRITE:
            return Permission.READ_WRITE
        elif permission == ProtoPermission.NONE:
            return Permission.NONE
        else:
            pass

    @staticmethod
    def to_protobuf(permission):
        """Convert Permission enum to protobuf permission.

        :param permission: The Permission enum to be converted.
        :return: The corresponding protobuf permission.
        """
        if permission == Permission.READ:
            return ProtoPermission.READ
        elif permission == Permission.WRITE:
            return ProtoPermission.WRITE
        elif permission == Permission.READ_WRITE:
            return ProtoPermission.READ_WRITE
        else:
            pass

    @staticmethod
    def is_writable(permission):
        """Check if the permission is writable.

        :param permission: The Permission enum to be checked.
        :return: True if the permission is writable, False otherwise.
        """
        if permission in [Permission.WRITE, Permission.READ_WRITE]:
            return True
        else:
            return False

    @staticmethod
    def is_readable(permission):
        """Check if the permission is readable.

        :param permission: The Permission enum to be checked.
        :return: True if the permission is readable, False otherwise.
        """
        if permission in [Permission.READ, Permission.READ_WRITE]:
            return True
        else:
            return False


class MessageType(Enum):
    """Enumeration of supported message types."""
    NORMAL = 0
    FIFO = 1
    DELAY = 2
    TRANSACTION = 3


class MessageTypeHelper:
    """Helper class for converting message types to protobuf and vice versa."""

    @staticmethod
    def from_protobuf(message_type):
        """Convert protobuf message type to MessageType enum.

        :param message_type: The protobuf message type to be converted.
        :return: The corresponding MessageType enum.
        """
        if message_type == ProtoMessageType.NORMAL:
            return MessageType.NORMAL
        elif message_type == ProtoMessageType.FIFO:
            return MessageType.FIFO
        elif message_type == ProtoMessageType.DELAY:
            return MessageType.DELAY
        elif message_type == ProtoMessageType.TRANSACTION:
            return MessageType.TRANSACTION
        else:
            pass

    @staticmethod
    def to_protobuf(message_type):
        """Convert MessageType enum to protobuf message type.

        :param message_type: The MessageType enum to be converted.
        :return: The corresponding protobuf message type.
        """
        if message_type == MessageType.NORMAL:
            return ProtoMessageType.NORMAL
        elif message_type == MessageType.FIFO:
            return ProtoMessageType.FIFO
        elif message_type == MessageType.DELAY:
            return ProtoMessageType.DELAY
        elif message_type == MessageType.TRANSACTION:
            return ProtoMessageType.TRANSACTION
        else:
            return ProtoMessageType.UNSPECIFIED


class MessageQueue:
    """A class that encapsulates a message queue entity."""

    def __init__(self, message_queue):
        """Initialize a MessageQueue instance.

        :param message_queue: The initial message queue to be encapsulated.
        """
        self._topic_resource = Resource(message_queue.topic.name, message_queue.topic)
        self.queue_id = message_queue.id
        self.permission = PermissionHelper.from_protobuf(message_queue.permission)
        self.accept_message_types = [
            MessageTypeHelper.from_protobuf(mt)
            for mt in message_queue.accept_message_types
        ]
        self.broker = Broker(message_queue.broker)

    @property
    def topic(self):
        """The topic resource name.

        :return: The name of the topic resource.
        """
        return self._topic_resource.name

    def __str__(self):
        """Get a string representation of the MessageQueue instance.

        :return: A string that represents the MessageQueue instance.
        """
        return f"{self.broker.name}.{self._topic_resource}.{self.queue_id}"

    def to_protobuf(self):
        """Convert the MessageQueue instance to protobuf message queue.

        :return: A protobuf message queue that represents the MessageQueue instance.
        """
        message_types = [
            MessageTypeHelper.to_protobuf(mt) for mt in self.accept_message_types
        ]
        return ProtoMessageQueue(
            topic=self._topic_resource.to_protobuf(),
            id=self.queue_id,
            permission=PermissionHelper.to_protobuf(self.permission),
            broker=self.broker.to_protobuf(),
            accept_message_types=message_types,
        )


class TopicRouteData:
    """A class that encapsulates a list of message queues."""

    def __init__(self, message_queues: List[definition_pb2.MessageQueue]):
        """Initialize a TopicRouteData instance.

        :param message_queues: The initial list of message queues to be encapsulated.
        """
        message_queue_list = []
        for mq in message_queues:
            message_queue_list.append(MessageQueue(mq))
        self.__message_queue_list = message_queue_list

    @property
    def message_queues(self) -> List[MessageQueue]:
        """The list of MessageQueue instances.

        :return: The list of MessageQueue instances that the TopicRouteData instance encapsulates.
        """
        return self.__message_queue_list
