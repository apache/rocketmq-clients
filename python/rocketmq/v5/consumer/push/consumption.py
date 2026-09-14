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
from concurrent.futures import ThreadPoolExecutor

from rocketmq.v5.consumer.push.message_listener import ConsumeResult
from rocketmq.v5.log import logger


class Consumption:
    """Coordinate concurrent, non-FIFO message consumption.

    Each message is submitted independently to the consumption thread pool.
    Listener results are recorded through client metrics and then forwarded to
    the consumer callback for acknowledgement or retry handling.
    """

    def __init__(self, message_listener, consumption_thread_count, consume_result_callback, client_id, backoff_policy):
        """Initialize the non-FIFO consumption coordinator.

        Args:
            message_listener: Listener invoked to consume each message.
            consumption_thread_count: Maximum number of consumption threads.
            consume_result_callback: Callback that handles each consumption result.
            client_id: Client identifier used in consumption thread names.
            backoff_policy: Retry policy exposed for broker redelivery handling.
        """
        self.__fifo = False
        self.__message_listener = message_listener
        self.__consumption_executor = ThreadPoolExecutor(max_workers=consumption_thread_count,
                                                         thread_name_prefix=f"{client_id}_message_consumption_thread")
        self.__consume_result_callback = consume_result_callback
        self.__backoff_policy = backoff_policy

    def execute_consume(self, messages, message_queue, process_queue, consumer_group, client_metrics):
        """Submit each message for independent concurrent consumption.

        Args:
            messages: Messages to consume.
            message_queue: Broker message queue from which the messages arrived.
            process_queue: Local process queue that caches the messages.
            consumer_group: Consumer group used when recording metrics.
            client_metrics: Metrics recorder for consumption latency and results.
        """
        for message in messages:
            self.__consumption_executor.submit(functools.partial(self.__consume, message=message, message_queue=message_queue, process_queue=process_queue, consumer_group=consumer_group, client_metrics=client_metrics))

    def shutdown(self):
        """Shutdown the consumption executor after pending tasks complete."""
        if self.__consumption_executor:
            self.__consumption_executor.shutdown()

    def __consume(self, message, message_queue, process_queue, consumer_group, client_metrics):
        """Consume one message and report its result.

        Corrupted messages and listener exceptions are reported as failures.

        Args:
            message: Message to consume.
            message_queue: Broker message queue associated with the message.
            process_queue: Local process queue that caches the message.
            consumer_group: Consumer group used when recording metrics.
            client_metrics: Metrics recorder for consumption latency and results.
        """
        consume_context = None
        try:
            consume_context = client_metrics.consume_before(consumer_group, message)
            if message.corrupted:
                logger.error(
                    f"message is corrupted for consumption, prepare to discard it, topic: {message.topic}, message_id: {message.message_id}")
                client_metrics.consume_after(consume_context, False)
                self.__consume_result_callback(ConsumeResult.FAILURE, message, message_queue, process_queue)
                return
            consume_result = self.__message_listener.consume(message)
        except Exception as e:
            logger.error(f"message listener raised an exception while consuming messages, topic: {message.topic}, message_id: {message.message_id}, {e}")
            consume_result = ConsumeResult.FAILURE
        client_metrics.consume_after(consume_context, consume_result == ConsumeResult.SUCCESS)
        self.__consume_result_callback(consume_result, message, message_queue, process_queue)

    @property
    def fifo(self):
        """Whether this coordinator performs FIFO consumption."""
        return False

    @property
    def backoff_policy(self):
        """Retry policy exposed for broker redelivery handling."""
        return self.__backoff_policy

    @backoff_policy.setter
    def backoff_policy(self, backoff_policy):
        """Update the retry policy used for broker redelivery handling.

        Args:
            backoff_policy: New retry policy.
        """
        self.__backoff_policy = backoff_policy
