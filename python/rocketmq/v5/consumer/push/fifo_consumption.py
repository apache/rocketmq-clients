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

from rocketmq.v5.consumer.push.message_listener import ConsumeResult
from rocketmq.v5.log import logger


class FifoConsumption:
    """Coordinate ordered consumption for FIFO messages.

    Within each received batch, messages are grouped by message group. Every
    group is submitted as one task and consumed sequentially, while different
    group tasks may run concurrently. Listener failures are retried in the same
    task before consumption proceeds to the next message. Corrupted messages
    bypass the listener and are reported as failures without local retries.
    """

    def __init__(self, message_listener, consumption_thread_count, consume_result_callback, client_id, backoff_policy):
        """Initialize the FIFO consumption coordinator.

        Args:
            message_listener: Listener invoked to consume each message.
            consumption_thread_count: Maximum number of consumption threads.
            consume_result_callback: Callback that handles each consumption result.
            client_id: Client identifier used in consumption thread names.
            backoff_policy: Retry policy used for local FIFO reconsumption.
        """
        self.__fifo = True
        self.__message_listener = message_listener
        self.__consumption_executor = ThreadPoolExecutor(max_workers=consumption_thread_count,
                                                         thread_name_prefix=f"{client_id}_message_consumption_thread")
        self.__consume_result_callback = consume_result_callback
        self.__backoff_policy = backoff_policy

    def execute_consume(self, messages, message_queue, process_queue, consumer_group, client_metrics):
        """Group messages and submit ordered consumption tasks.

        Messages with the same message group are consumed sequentially in one
        task. Messages without a group are collected into a separate task.

        Args:
            messages: Messages to consume.
            message_queue: Broker message queue from which the messages arrived.
            process_queue: Local process queue that caches the messages.
            consumer_group: Consumer group used when recording metrics.
            client_metrics: Metrics recorder for consumption latency and results.
        """
        messages_by_group = defaultdict(list)
        messages_without_group = []
        for message in messages:
            if message.message_group:
                messages_by_group[message.message_group].append(message)
            else:
                messages_without_group.append(message)

        if messages_by_group:
            for group_msgs in messages_by_group.values():
                self.__submit_fifo_consume_task(group_msgs, message_queue, process_queue, consumer_group, client_metrics)
        if messages_without_group:
            self.__submit_fifo_consume_task(messages_without_group, message_queue, process_queue, consumer_group, client_metrics)

    def shutdown(self):
        """Shutdown the consumption executor after pending tasks complete."""
        if self.__consumption_executor:
            self.__consumption_executor.shutdown()

    def __submit_fifo_consume_task(self, messages, message_queue, process_queue, consumer_group, client_metrics):
        """Submit one ordered message sequence to the consumption executor.

        Args:
            messages: Ordered messages to consume in one task.
            message_queue: Broker message queue associated with the messages.
            process_queue: Local process queue that caches the messages.
            consumer_group: Consumer group used when recording metrics.
            client_metrics: Metrics recorder for consumption latency and results.
        """
        if len(messages) <= 0:
            logger.debug(f"{message_queue} consume end, because receive messages is 0.")
            return

        self.__consumption_executor.submit(
            functools.partial(self.__consume_fifo, messages=messages, message_queue=message_queue, process_queue=process_queue, consumer_group=consumer_group, client_metrics=client_metrics)
        )

    def __consume_fifo(self, messages, message_queue, process_queue, consumer_group, client_metrics):
        """Consume an ordered message sequence and report each final result.

        Metrics for a message cover the complete local retry sequence, and the
        result callback is invoked once with the final consumption result.

        Args:
            messages: Ordered messages to consume.
            message_queue: Broker message queue associated with the messages.
            process_queue: Local process queue that caches the messages.
            consumer_group: Consumer group used when recording metrics.
            client_metrics: Metrics recorder for consumption latency and results.
        """
        for message in messages:
            try:
                consume_context = client_metrics.consume_before(consumer_group, message)
                if message.corrupted:
                    logger.error(f"message is corrupted for consumption, prepare to discard it, topic: {message.topic}, message_id: {message.message_id}")
                    client_metrics.consume_after(consume_context, False)
                    self.__consume_result_callback(ConsumeResult.FAILURE, message, message_queue, process_queue)
                else:
                    try:
                        consume_result = self.__consume_message(message)
                    except Exception as consume_exception:
                        logger.error(f"[BUG]consume fifo raise exception, topic: {message.topic}, message_id: {message.message_id}, {consume_exception}")
                        client_metrics.consume_after(consume_context, False)
                        self.__consume_result_callback(ConsumeResult.FAILURE, message, message_queue, process_queue)
                        continue
                    client_metrics.consume_after(consume_context, consume_result == ConsumeResult.SUCCESS)
                    self.__consume_result_callback(consume_result, message, message_queue, process_queue)
            except Exception as e:
                logger.error(f"failed to consume message, topic: {message.topic}, message_id: {message.message_id}, {e}")

    def __consume_message(self, message, attempt=1):
        """Consume one FIFO message with local backoff retries.

        Args:
            message: Message to consume.
            attempt: Current 1-based consumption attempt.

        Returns:
            The final :class:`ConsumeResult`, or ``FAILURE`` if the listener
            keeps failing or raising exceptions.
        """
        consume_result = ConsumeResult.FAILURE
        try:
            consume_result = self.__message_listener.consume(message)
        except Exception as e:
            logger.error(f"consume message in fifo raise exception, {e}")

        if consume_result == ConsumeResult.SUCCESS:
            return consume_result

        # reconsume
        max_attempts = self.__backoff_policy.max_attempts
        attempt_delay = self.__backoff_policy.get_next_attempt_delay(attempt)
        if attempt >= max_attempts:
            return consume_result
        time.sleep(attempt_delay)
        return self.__consume_message(message, attempt + 1)

    @property
    def fifo(self):
        """Whether this coordinator performs FIFO consumption."""
        return True

    @property
    def backoff_policy(self):
        """Retry policy used for local FIFO reconsumption."""
        return self.__backoff_policy

    @backoff_policy.setter
    def backoff_policy(self, backoff_policy):
        """Update the retry policy used for local FIFO reconsumption.

        Args:
            backoff_policy: New retry policy.
        """
        self.__backoff_policy = backoff_policy
