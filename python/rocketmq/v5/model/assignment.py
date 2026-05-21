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

    def __init__(self, message_queue):
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
        return self.__message_queue


class Assignments:

    def __init__(self, assignments):
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
        return [queue for queue in left.message_queues() if queue not in set(right.message_queues())]

    @property
    def assignments(self):
        return self.__assignments

    def message_queues(self):
        return list(
            map(lambda assignment: assignment.message_queue, self.__assignments)
        )
