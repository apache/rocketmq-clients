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

from rocketmq.message_queue import MessageQueue


class TopicRouteData:
    def __init__(self, message_queues):
        self.message_queues = [MessageQueue(mq) for mq in message_queues]

    def __eq__(self, other):
        if other is None:
            return False
        if self is other:
            return True
        return self.message_queues == other.message_queues

    def __hash__(self):
        return hash(tuple(self.message_queues))

    def __str__(self):
        mqs = ', '.join(str(mq) for mq in self.message_queues)
        return f"[{mqs}]"
