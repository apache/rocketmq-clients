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

import hashlib
import random

from rocketmq.v5.exception import IllegalArgumentException
from rocketmq.v5.model import TopicRouteData
from rocketmq.v5.util import AtomicInteger


class QueueSelector:

    NONE_TYPE_SELECTOR = 0
    PRODUCER_QUEUE_SELECTOR = 1
    SIMPLE_CONSUMER_QUEUE_SELECTOR = 2

    def __init__(self, queues, selector_type=NONE_TYPE_SELECTOR):
        self.__index = AtomicInteger(random.randint(1, 1000))
        self.__message_queues = queues
        self.__selector_type = selector_type

    @classmethod
    def producer_queue_selector(cls, topic_route: TopicRouteData):
        return cls(
            list(
                filter(
                    lambda queue: queue.is_writable() and queue.is_master_broker(),
                    topic_route.message_queues,
                )
            ),
            QueueSelector.PRODUCER_QUEUE_SELECTOR,
        )

    @classmethod
    def simple_consumer_queue_selector(cls, topic_route: TopicRouteData):
        return cls(
            list(
                filter(
                    lambda queue: queue.is_readable() and queue.is_master_broker(),
                    topic_route.message_queues,
                )
            ),
            QueueSelector.SIMPLE_CONSUMER_QUEUE_SELECTOR,
        )

    def select_next_queue(self):
        if self.__selector_type == QueueSelector.NONE_TYPE_SELECTOR:
            raise IllegalArgumentException(
                "error type for queue selector, type is NONE_TYPE_SELECTOR."
            )
        return self.__message_queues[
            self.__index.get_and_increment() % len(self.__message_queues)
        ]

    def select_queue_by_hash_key(self, key):
        hash_object = hashlib.sha256(key.encode('utf-8'))
        hash_code = int.from_bytes(hash_object.digest(), byteorder='big')
        return self.__message_queues[hash_code % len(self.__message_queues)]

    def all_queues(self):
        index = self.__index.get_and_increment() % len(self.__message_queues)
        return self.__message_queues[index:] + self.__message_queues[:index]

    def update(self, topic_route: TopicRouteData):
        if topic_route.message_queues == self.__message_queues:
            return
        if self.__selector_type == QueueSelector.PRODUCER_QUEUE_SELECTOR:
            self.__message_queues = list(
                filter(
                    lambda queue: queue.is_writable() and queue.is_master_broker(),
                    topic_route.message_queues,
                )
            )
        elif self.__selector_type == QueueSelector.SIMPLE_CONSUMER_QUEUE_SELECTOR:
            self.__message_queues = list(
                filter(
                    lambda queue: queue.is_readable() and queue.is_master_broker(),
                    topic_route.message_queues,
                )
            )
