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

from rocketmq.grpc_protocol import Permission, definition_pb2
from rocketmq.v5.client.connection import RpcEndpoints
from rocketmq.v5.model import Message


class MessageQueue:
    """Wrapper representing a message queue on a specific broker.

    Encapsulates queue metadata including topic, broker identity, permission
    level, and accepted message types. Used by producers to select writable
    queues and by consumers to select readable queues.
    """
    MASTER_BROKER_ID = 0

    def __init__(self, queue):
        """Create a MessageQueue from a protobuf MessageQueue.

        Args:
            queue: A gRPC ``MessageQueue`` protobuf message.
        """
        self.__topic = queue.topic.name
        self.__namespace = queue.topic.resource_namespace
        self.__queue_id = queue.id
        self.__permission = queue.permission
        self.__broker_name = queue.broker.name
        self.__broker_id = queue.broker.id
        self.__broker_endpoints = RpcEndpoints(queue.broker.endpoints)
        self.__accept_message_types = set(queue.accept_message_types)

    def is_readable(self):
        """Check if the queue permits read operations.

        Returns:
            ``True`` if the permission is ``READ`` or ``READ_WRITE``.
        """
        return (
            self.__permission == Permission.READ
            or self.__permission == Permission.READ_WRITE
        )

    def is_writable(self):
        """Check if the queue permits write operations.

        Returns:
            ``True`` if the permission is ``WRITE`` or ``READ_WRITE``.
        """
        return (
            self.__permission == Permission.WRITE
            or self.__permission == Permission.READ_WRITE
        )

    def is_master_broker(self):
        """Check if the queue belongs to the master broker (broker ID 0).

        Returns:
            ``True`` if the broker is the master node.
        """
        return self.__broker_id == MessageQueue.MASTER_BROKER_ID

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MessageQueue):
            return False
        ret = (
            self.__topic == other.__topic
            and self.__namespace == other.__namespace
            and self.__queue_id == other.__queue_id
            and self.__permission == other.__permission
            and self.__broker_name == other.__broker_name
            and self.__broker_id == other.__broker_id
            and self.__broker_endpoints == other.__broker_endpoints
            and sorted(self.__accept_message_types)
            == sorted(other.__accept_message_types)
        )
        return ret

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, MessageQueue):
            return NotImplemented
        if self.__broker_name < other.__broker_name:
            return True
        elif self.__broker_id < other.__broker_id:
            return True
        elif self.__queue_id < other.__queue_id:
            return True
        return False

    def __str__(self):
        return f"{self.__broker_name}.{self.__topic}.{self.__queue_id}"

    def __hash__(self):
        return hash((self.__broker_name, self.__topic, self.__queue_id))

    def message_queue0(self):
        """Convert back to a protobuf ``MessageQueue`` for RPC requests.

        Returns:
            A ``definition_pb2.MessageQueue`` instance.
        """
        queue = definition_pb2.MessageQueue()  # noqa
        queue.topic.name = self.__topic
        queue.topic.resource_namespace = self.__namespace
        queue.id = self.__queue_id
        queue.permission = self.__permission
        queue.broker.name = self.__broker_name
        queue.broker.id = self.__broker_id
        queue.broker.endpoints.CopyFrom(self.__broker_endpoints.endpoints)
        queue.accept_message_types.extend(self.__accept_message_types)
        return queue

    def accept_message_types_desc(self):
        """Get a comma-separated description of accepted message types.

        Returns:
            Human-readable string like ``"NORMAL,DELAY"`` or empty string.
        """
        ret = ""
        for access_type in self.__accept_message_types:
            ret = ret + Message.message_type_desc(access_type) + ","
        if len(ret) == 0:
            return ret
        else:
            return ret[:len(ret) - 1]

    @property
    def endpoints(self) -> RpcEndpoints:
        """The broker's RPC endpoint addresses for this queue."""
        return self.__broker_endpoints

    @property
    def accept_message_types(self):
        """Set of accepted message type integers (e.g., normal, transaction, delay)."""
        return self.__accept_message_types

    @property
    def topic(self):
        """The topic name this queue belongs to."""
        return self.__topic


class TopicRouteData:
    """Aggregated route information for a single topic.

    Holds all ``MessageQueue`` instances for a topic across brokers,
    enabling queue selection for both producers and consumers.
    """

    def __init__(self, message_queues):
        """Build route data from protobuf message queues.

        Args:
            message_queues: List of gRPC ``MessageQueue`` protobuf messages.
        """
        self.__message_queues = list(
            map(lambda queue: MessageQueue(queue), message_queues)
        )

    def __eq__(self, other):
        if self is other:
            return True
        if other is None or not isinstance(other, TopicRouteData):
            return False
        return self.__message_queues == other.__message_queues

    def __hash__(self):
        return hash(tuple(self.__message_queues))

    def __str__(self):
        return (
            "message_queues:("
            + ", ".join(str(queue) for queue in self.__message_queues)
            + ")"
        )

    def all_endpoints(self):
        """Collect all unique broker endpoints from the route.

        Returns:
            Dict mapping endpoint facade string to :class:`RpcEndpoints`.
        """
        endpoints_map = {}
        for queue in self.__message_queues:
            endpoints_map[queue.endpoints.facade] = queue.endpoints
        return endpoints_map

    @property
    def message_queues(self):
        """The list of :class:`MessageQueue` instances for this topic."""
        return self.__message_queues
