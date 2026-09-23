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

from rocketmq.v5.model import MessageQueue


class Assignment:
    """Represents a single queue assigned to a consumer instance.

    Wraps a :class:`MessageQueue` that has been allocated to this consumer
    by the broker's load balancer.
    """

    def __init__(self, message_queue):
        """Create an assignment.

        Args:
            message_queue: The :class:`MessageQueue` assigned to this consumer.
        """
        self.__message_queue = message_queue

    def __str__(self):
        return str(self.__message_queue)

    def __eq__(self, other):
        return self.__message_queue == other.message_queue

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, Assignment):
            return NotImplemented
        return self.__message_queue < other.message_queue

    @property
    def message_queue(self):
        """The assigned :class:`MessageQueue`."""
        return self.__message_queue


class Assignments:
    """Collection of queue assignments allocated to a consumer.

    Wraps a list of :class:`Assignment` objects returned by the broker,
    providing convenience methods for queue comparison and extraction.
    """

    def __init__(self, assignments):
        """Build assignments from protobuf data.

        Args:
            assignments: List of gRPC assignment protobuf messages.
        """
        self.__assignments = list(
            map(lambda assignment: Assignment(MessageQueue(assignment.message_queue)), assignments)
        )

    def __str__(self):
        if not self.__assignments:
            return "None"
        assignment_strs = ", ".join(str(assignment) for assignment in self.__assignments)
        return f"{assignment_strs}"

    def __eq__(self, other):
        return sorted(self.__assignments) == sorted(other.assignments)

    @staticmethod
    def diff_queues(left, right):
        """Find queues present in ``left`` but not in ``right``.

        Args:
            left: An :class:`Assignments` representing the current set.
            right: An :class:`Assignments` representing the target set.

        Returns:
            List of :class:`MessageQueue` objects that are in ``left`` but missing from ``right``.
        """
        return [queue for queue in left.message_queues() if queue not in set(right.message_queues())]

    @property
    def assignments(self):
        """The list of :class:`Assignment` objects."""
        return self.__assignments

    def message_queues(self):
        """Extract all :class:`MessageQueue` objects from the assignments.

        Returns:
            List of :class:`MessageQueue` instances.
        """
        return list(
            map(lambda assignment: assignment.message_queue, self.__assignments)
        )
