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

import threading
import time

from rocketmq.v5.log import logger
from rocketmq.v5.util import AtomicInteger


class ProcessQueue:

    def __init__(self, message_queue):
        self.__message_queue = message_queue
        self.__cached_messages_bytes = AtomicInteger()
        self.__cached_messages_count = AtomicInteger()
        self.__reception_times = AtomicInteger()
        self.__dropped = False
        self.__cache_message_lock = threading.Lock()
        self.__active_time = int(time.time())  # seconds
        self.__cache_full_time = -1  # seconds

    def __str__(self):
        return f"{self.__message_queue}"

    def max_receive_batch_size(self, queue_count_threshold, default): # noqa
        self.__active_time = int(time.time())  # begin to receive
        return min(max(1, queue_count_threshold - self.__cached_messages_count.get()), default)

    def drop(self):
        self.__dropped = True

    def cache_messages(self, messages):
        total_size = 0
        with self.__cache_message_lock:
            for message in messages:
                total_size += len(message.body)
            self.__cached_messages_bytes.add_and_get(total_size)
            count = len(messages)
            self.__cached_messages_count.add_and_get(count)

    def evict_message(self, message):
        with self.__cache_message_lock:
            self.__cached_messages_count.decrement_and_get()
            self.__cached_messages_bytes.subtract_and_get(len(message.body))

    def is_cache_full(self, queue_count_threshold, queue_bytes_threshold):
        if self.__cached_messages_count.get() >= queue_count_threshold:
            logger.warn(f"process queue total cached messages exceeds the threshold, "
                        f"threshold: {queue_count_threshold}, actual: {self.__cached_messages_count.get()}, queue: {self.__message_queue}")
            self.__cache_full_time = int(time.time())
            return True
        elif self.__cached_messages_bytes.get() >= queue_bytes_threshold:
            logger.warn(f"process queue total cached message size exceeds the threshold, "
                        f"threshold: {queue_bytes_threshold}, actual: {self.__cached_messages_bytes.get()}, queue: {self.__message_queue}")
            self.__cache_full_time = int(time.time())
            return True
        return False

    def expired(self, long_polling_timeout, request_timeout):
        max_idle_duration = (long_polling_timeout + request_timeout) * 3
        idle_duration = int(time.time()) - self.__active_time
        if idle_duration < 0:
            return False
        after_cache_full_duration = int(time.time()) - self.__cache_full_time
        if after_cache_full_duration - max_idle_duration < 0:
            return False
        logger.warn(f"process queue is idle, idle_duration: {idle_duration}, max_idle_duration: {max_idle_duration}, after_cache_full_duration: {after_cache_full_duration}, mq: {self.__message_queue}")
        return True

    """ property """

    @property
    def message_queue(self):
        return self.__message_queue

    @property
    def dropped(self):
        return self.__dropped

    @property
    def cached_messages_count(self):
        return self.__cached_messages_count.get()

    @property
    def cached_messages_bytes(self):
        return self.__cached_messages_bytes.get()
