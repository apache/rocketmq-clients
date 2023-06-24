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

from protocol.definition_pb2 import Broker as ProtoBroker
from protocol.definition_pb2 import MessageQueue as ProtoMessageQueue
from protocol.definition_pb2 import MessageType as ProtoMessageType
from protocol.definition_pb2 import Permission as ProtoPermission
from protocol.definition_pb2 import Resource as ProtoResource

from rocketmq.rpc_client import Endpoints


class Broker:
    def __init__(self, broker):
        self.name = broker.name
        self.id = broker.id
        self.endpoints = Endpoints(broker.endpoints)

    def to_protobuf(self):
        return ProtoBroker(
            Name=self.name,
            Id=self.id,
            Endpoints=self.endpoints.to_protobuf()
        )


class Resource:
    def __init__(self, name=None, resource=None):
        if resource is not None:
            self.namespace = resource.ResourceNamespace
            self.name = resource.Name
        else:
            self.namespace = ""
            self.name = name

    def to_protobuf(self):
        return ProtoResource(
            ResourceNamespace=self.namespace,
            Name=self.name
        )

    def __str__(self):
        return f"{self.namespace}.{self.name}" if self.namespace else self.name


class Permission(Enum):
    NONE = 0
    Read = 1
    Write = 2
    ReadWrite = 3


class PermissionHelper:
    @staticmethod
    def from_protobuf(permission):
        if permission == ProtoPermission.READ:
            return Permission.Read
        elif permission == ProtoPermission.WRITE:
            return Permission.Write
        elif permission == ProtoPermission.READ_WRITE:
            return Permission.ReadWrite
        elif permission == ProtoPermission.NONE:
            return Permission.NONE
        else:
            pass

    @staticmethod
    def to_protobuf(permission):
        if permission == Permission.Read:
            return ProtoPermission.READ
        elif permission == Permission.Write:
            return ProtoPermission.WRITE
        elif permission == Permission.ReadWrite:
            return ProtoPermission.READ_WRITE
        else:
            pass

    @staticmethod
    def is_writable(permission):
        if permission in [Permission.Write, Permission.ReadWrite]:
            return True
        else:
            return False

    @staticmethod
    def is_readable(permission):
        if permission in [Permission.Read, Permission.ReadWrite]:
            return True
        else:
            return False


class MessageType(Enum):
    Normal = 0
    Fifo = 1
    Delay = 2
    Transaction = 3


class MessageTypeHelper:
    @staticmethod
    def from_protobuf(message_type):
        if message_type == ProtoMessageType.NORMAL:
            return MessageType.Normal
        elif message_type == ProtoMessageType.FIFO:
            return MessageType.Fifo
        elif message_type == ProtoMessageType.DELAY:
            return MessageType.Delay
        elif message_type == ProtoMessageType.TRANSACTION:
            return MessageType.Transaction
        else:
            pass

    @staticmethod
    def to_protobuf(message_type):
        if message_type == MessageType.Normal:
            return ProtoMessageType.Normal
        elif message_type == MessageType.Fifo:
            return ProtoMessageType.Fifo
        elif message_type == MessageType.Delay:
            return ProtoMessageType.Delay
        elif message_type == MessageType.Transaction:
            return ProtoMessageType.Transaction
        else:
            return ProtoMessageType.Unspecified


class MessageQueue:
    def __init__(self, message_queue):
        self._topic_resource = Resource(message_queue.topic)
        self.queue_id = message_queue.id
        self.permission = PermissionHelper.from_protobuf(
            message_queue.permission)
        self.accept_message_types = [MessageTypeHelper.from_protobuf(mt) for
                                     mt in message_queue.accept_message_types]
        self.broker = Broker(message_queue.broker)

    @property
    def topic(self):
        return self._topic_resource.name

    def __str__(self):
        return f"{self.broker.name}.{self._topic_resource}.{self.queue_id}"

    def to_protobuf(self):
        message_types = [MessageTypeHelper.to_protobuf(mt) for mt in
                         self.accept_message_types]
        return ProtoMessageQueue(
            topic=self._topic_resource.to_protobuf(),
            id=self.queue_id,
            permission=PermissionHelper.to_protobuf(self.permission),
            broker=self.broker.to_protobuf(),
            accept_message_types=message_types,
        )
