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
from concurrent.futures import Future
from typing import Optional

from rocketmq.grpc_protocol import ClientType
from rocketmq.v5.client import ClientConfiguration
from rocketmq.v5.client.balancer import QueueSelector
from rocketmq.v5.consumer.consumer import Consumer
from rocketmq.v5.exception import (IllegalArgumentException,
                                   IllegalStateException)
from rocketmq.v5.log import logger
from rocketmq.v5.util import AtomicInteger, ConcurrentMap


class SimpleConsumer(Consumer):
    """A pull-based consumer that receives messages on demand.

    Unlike :class:`PushConsumer`, the user actively calls :meth:`receive` to
    fetch messages from subscribed topics. Suitable for scenarios where
    message consumption needs to be controlled by the application's own
    scheduling or event loop.
    """

    def __init__(
        self,
        client_configuration: ClientConfiguration,
        consumer_group,
        subscription: Optional[dict] = None,
        await_duration=20,
        client_type=ClientType.SIMPLE_CONSUMER,
        tls_enable=False
    ):
        """Initialize the SimpleConsumer.

        Args:
            client_configuration: Connection and authentication configuration.
            consumer_group: The consumer group name.
            subscription: Optional dict of ``{topic: FilterExpression}`` for initial subscriptions.
            await_duration: Long polling timeout in seconds (default: 20).
            client_type: The :class:`ClientType` for this consumer.
            tls_enable: Whether to enable TLS.

        Raises:
            IllegalArgumentException: If await_duration is None.
        """
        if await_duration is None:
            raise IllegalArgumentException("awaitDuration should not be null")

        super().__init__(
            client_configuration,
            consumer_group,
            client_type,
            subscription,
            tls_enable
        )

        self.__await_duration = await_duration  # long polling timeout, seconds
        # <String /* topic */, Int /* index */>
        self.__receive_queue_selectors = ConcurrentMap()
        self.__topic_index = AtomicInteger(0)
        self.__queue_index_lock = threading.Lock()

    def receive(self, max_message_num, invisible_duration):
        """Receive messages synchronously from subscribed topics.

        Performs long polling up to ``await_duration`` seconds if no messages
        are immediately available. Messages are returned in batches.

        Args:
            max_message_num: Maximum number of messages to receive in one call.
            invisible_duration: The duration (in seconds) that received messages
                will be invisible to other consumers in the same group. After
                this duration, unacknowledged messages become visible again.

        Returns:
            A list of :class:`Message` objects.

        Raises:
            IllegalStateException: If consumer is not running.
            IllegalArgumentException: If max_message_num <= 0 or no subscriptions.
        """

        return self.__receive_message(max_message_num, invisible_duration)

    def receive_async(self, max_message_num, invisible_duration):
        """Receive messages asynchronously, returning a Future.

        Args:
            max_message_num: Maximum number of messages to receive.
            invisible_duration: Duration in seconds for message invisibility.

        Returns:
            A ``concurrent.futures.Future`` that resolves to a list of :class:`Message`.
        """

        return self.__receive_message(max_message_num, invisible_duration, sync=False)

    def shutdown(self):
        """Shutdown the SimpleConsumer and release all resources.

        Stops all schedulers and closes gRPC connections.

        Raises:
            IllegalStateException: If consumer is not running or already shutdown.
        """
        logger.info(f"begin to shutdown {self}.")
        super().shutdown()
        logger.info(f"shutdown {self} success.")

    def reset_setting(self, settings):
        if not self._init_settings_event.is_set():
            self._init_settings_event.set()

    # def _sync_setting_req(self, endpoints):
    def sync_setting_req(self, endpoints):
        req = super().sync_setting_req(endpoints)
        req.settings.subscription.long_polling_timeout.seconds = self.__await_duration
        return req

    # def _update_queue_selector(self, topic, topic_route):
    def update_queue_selector(self, topic, topic_route):
        queue_selector = self.__receive_queue_selectors.get(topic)
        if queue_selector is None:
            return
        queue_selector.update(topic_route)

    def ack(self, message):
        """Acknowledge a message, marking it as successfully consumed.

        Once acknowledged, the message will not be redelivered to any consumer
        in the same group.

        Args:
            message: The :class:`Message` to acknowledge.
        """
        self._ack(message)

    def ack_async(self, message):
        """Acknowledge a message asynchronously, returning a Future.

        Args:
            message: The :class:`Message` to acknowledge.

        Returns:
            A ``concurrent.futures.Future`` for the ack result.
        """
        self._ack_async(message)

    def change_invisible_duration(self, message, invisible_duration):
        """Change the invisible duration of a received message.

        Use this when the consumer needs more time to process a message than
        the original invisible duration. The message will remain invisible to
        other consumers for the new duration.

        Args:
            message: The :class:`Message` to change invisible duration for.
            invisible_duration: New invisible duration in seconds.
        """
        self._change_invisible_duration(message, invisible_duration)

    def change_invisible_duration_async(self, message, invisible_duration):
        """Change the invisible duration of a message asynchronously.

        Args:
            message: The :class:`Message` to change invisible duration for.
            invisible_duration: New invisible duration in seconds.

        Returns:
            A ``concurrent.futures.Future`` for the result.
        """
        self._change_invisible_duration_async(message, invisible_duration)

    def _on_start(self):
        logger.info(f"{self} start success.")

    def _on_start_failure(self):
        logger.info(f"{self} start failed.")

    def __select_topic_for_receive(self):
        try:
            # select the next topic for receive
            mod_index = self.__topic_index.get_and_increment() % len(
                self._subscriptions.keys()
            )
            return list(self._subscriptions.keys())[mod_index]
        except Exception as e:
            logger.error(
                f"simple consumer select topic for receive message exception: {e}"
            )
            raise e

    def __select_topic_queue(self, topic):
        try:
            route = self._retrieve_topic_route_data(topic)
            if self.client_type == ClientType.SIMPLE_CONSUMER:
                queue_selector = QueueSelector.simple_consumer_queue_selector(route)
            else:
                queue_selector = QueueSelector.lite_simple_consumer_queue_selector(route)
            self.__receive_queue_selectors.put_if_absent(topic, queue_selector)
            return queue_selector.select_next_queue()
        except Exception as e:
            logger.error(f"simple consumer select topic queue raise exception: {e}")
            raise e

    def __receive_message(self, max_message_num, invisible_duration, sync=True):
        self.__receive_pre_check(max_message_num)
        topic = self.__select_topic_for_receive()
        queue = self.__select_topic_queue(topic)
        req = self._receive_req(topic, queue, max_message_num, False, invisible_duration, self.__await_duration)
        timeout = self.client_configuration.request_timeout + self.__await_duration
        if sync:
            return self._receive(queue, req, timeout)
        else:
            return self._receive_async(queue, req, timeout, Future())

    def __receive_pre_check(self, max_message_num):
        if not self.is_running:
            raise IllegalStateException("consumer is not running now.")
        if len(self._subscriptions.keys()) == 0:
            raise IllegalArgumentException("There is no topic to receive message")
        if max_message_num <= 0:
            raise IllegalArgumentException("max_message_num must be greater than 0")

    @property
    def await_duration(self):
        """The long polling timeout in seconds for message receive."""
        return self.__await_duration

    @await_duration.setter
    def await_duration(self, await_duration):
        self.__await_duration = await_duration
