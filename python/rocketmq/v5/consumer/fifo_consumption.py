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

import functools
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

from rocketmq.v5.consumer.message_listener import ConsumeResult
from rocketmq.v5.log import logger
from rocketmq.v5.model.retry_policy import RetryPolicy


class FifoConsumption:

    def __init__(self, message_listener, consumption_thread_count, consume_result_callback, client_id, backoff_policy):
        self.__fifo = True
        self.__message_listener = message_listener
        self.__consumption_executor = ThreadPoolExecutor(max_workers=consumption_thread_count,
                                                         thread_name_prefix=f"{client_id}_message_consumption_thread")
        self.__consume_result_callback = consume_result_callback
        self.__backoff_policy = backoff_policy

    def execute_consume(self, messages, message_queue, consumer_group, client_metrics):
        messages_by_group = defaultdict(list)
        messages_without_group = []
        for message in messages:
            if message.message_group:
                messages_by_group[message.message_group].append(message)
            else:
                messages_without_group.append(message)

        if messages_by_group:
            for group_msgs in messages_by_group.values():
                self.__submit_fifo_consume_task(group_msgs, message_queue, consumer_group, client_metrics)
        if messages_without_group:
            self.__submit_fifo_consume_task(messages_without_group, message_queue, consumer_group, client_metrics)

    def __submit_fifo_consume_task(self, messages, message_queue, consumer_group, client_metrics):
        if len(messages) <= 0:
            logger.debug(f"{message_queue} consume end, because receive messages is 0.")
            return

        self.__consumption_executor.submit(
            functools.partial(self.__consume_fifo, messages=messages, message_queue=message_queue, consumer_group=consumer_group, client_metrics=client_metrics)
        )

    def __consume_fifo(self, messages, message_queue, consumer_group, client_metrics):
        for message in messages:
            try:
                consume_context = client_metrics.consume_before(consumer_group, message)
                if message.corrupted:
                    if message.corrupted:
                        logger.error(f"message is corrupted for consumption, prepare to discard it, topic: {message.topic}, message_id: {message.message_id}")
                    client_metrics.consume_after(consume_context, False)
                    self.__consume_result_callback(ConsumeResult.FAILURE, message, message_queue)
                else:
                    try:
                        consume_result = self.__consume_message(message)
                    except Exception as consume_exception:
                        logger.error(f"[BUG]consume fifo raise exception, topic: {message.topic}, message_id: {message.message_id}, {consume_exception}")
                        client_metrics.consume_after(consume_context, False)
                        self.__consume_result_callback(ConsumeResult.FAILURE, message, message_queue)
                        continue
                    client_metrics.consume_after(consume_context, consume_result == ConsumeResult.SUCCESS)
                    self.__consume_result_callback(consume_result, message, message_queue)
            except Exception as e:
                logger.error(f"failed to consume message, topic: {message.topic}, message_id: {message.message_id}, {e}")

    def __consume_message(self, message, attempt=1):
        consume_result = ConsumeResult.FAILURE
        try:
            consume_result = self.__message_listener.consume(message)
        except Exception as e:
            logger.error(f"consume message in fifo raise exception, {e}")

        if consume_result == ConsumeResult.SUCCESS:
            return consume_result

        # reconsume
        if self.__backoff_policy:
            max_attempts = self.__backoff_policy.max_attempts
            attempt_delay = self.__backoff_policy.get_next_attempt_delay(attempt)
        else:
            max_attempts = RetryPolicy.DEFAULT_MAX_ATTEMPTS
            attempt_delay = RetryPolicy.DEFAULT_RECONSUME_DELAY
        if attempt >= max_attempts:
            return consume_result
        time.sleep(attempt_delay)
        return self.__consume_message(message, attempt + 1)

    @property
    def fifo(self):
        return True

    @property
    def backoff_policy(self):
        return self.__backoff_policy

    @backoff_policy.setter
    def backoff_policy(self, backoff_policy):
        self.__backoff_policy = backoff_policy
