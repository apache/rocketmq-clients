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

from rocketmq.grpc_protocol import (Code, LiteSubscriptionAction,
                                    SyncLiteSubscriptionRequest)
from rocketmq.v5.client.scheduler import ClientScheduler
from rocketmq.v5.exception import (IllegalStateException,
                                   LiteSubscriptionQuotaExceededException)
from rocketmq.v5.log import logger
from rocketmq.v5.util import AtomicInteger, MessagingResultChecker


class LiteTopicSubscriptionManager:
    """Manages lite topic subscriptions for a Lite consumer.

    A lite consumer binds to a single physical topic (bind_topic) and can
    dynamically subscribe/unsubscribe to multiple lite topics within it.
    Lite topics share the same route and message queues of the bind topic,
    avoiding the overhead of creating separate topics on the server.

    The manager handles subscription synchronization with the server,
    quota enforcement, and periodic subscription refresh.
    """

    def __init__(self, consumer, bind_topic):
        """Initialize the lite topic subscription manager.

        Args:
            consumer: The LiteSimpleConsumer or LitePushConsumer instance.
            bind_topic: The physical topic that all lite topics share.
        """
        self.__consumer = consumer
        self.__bind_topic = bind_topic
        self.__lite_topics = set()
        self.__lite_subscription_quota = AtomicInteger(0)
        self.__max_lite_topic_size = AtomicInteger(64)
        self.__scheduler = None

    def set_config(self, settings):
        """Update subscription quota and size limits from server settings.

        Args:
            settings: The :class:`Settings` protobuf from the server, containing
                ``lite_subscription_quota`` and ``max_lite_topic_size``.
        """
        self.__lite_subscription_quota.set(settings.subscription.lite_subscription_quota)
        self.__max_lite_topic_size.set(settings.subscription.max_lite_topic_size)

    def start_scheduler(self, io_loop):
        """Start the periodic lite subscription sync scheduler.

        Every 30 seconds, syncs all subscribed lite topics to the server
        to ensure the broker has up-to-date subscription information.

        Args:
            io_loop: The asyncio event loop to run the scheduler on.
        """
        self.__scheduler = ClientScheduler(
            f"{self.__consumer.client_id}_sync_all_lite_subscription_scheduler_thread",
            self.sync_all_lite_subscription,
            30, 30,
            io_loop
        )
        self.__scheduler.start_scheduler()
        logger.info("start sync all lite subscription scheduler success.")

    def stop_scheduler(self):
        """Stop the periodic lite subscription sync scheduler."""
        if self.__scheduler:
            self.__scheduler.stop_scheduler()
            self.__scheduler = None

    def subscribe_lite(self, lite_topic):
        """Subscribe to a lite topic.

        The lite topic must pass pre-checks (not blank, within length limit,
        within quota). The subscription is synced to the server immediately.

        Args:
            lite_topic: The name of the lite topic to subscribe to.

        Raises:
            IllegalStateException: If consumer is not running, lite_topic is blank,
                or length exceeds max_lite_topic_size.
            LiteSubscriptionQuotaExceededException: If subscription quota is exceeded.
        """

        if not self.__consumer.is_running:
            raise IllegalStateException("unable to add lite subscription because consumer is not running")
        self.__subscribe_lite_pre_check(lite_topic)
        try:
            request = self.__add_lite_subscription_req(lite_topic, self.__consumer.consumer_group, self.__consumer.client_configuration)
            res = self.__consumer.rpc_client.sync_lite_subscription_async(
                self.__consumer.client_configuration.rpc_endpoints, request, metadata=self.__consumer.sign(), timeout=self.__consumer.client_configuration.request_timeout,
            ).result()
            self.__handle_lite_subscription_response(request, res, lite_topic)
            logger.info(f"[{self.__consumer}] subscribe lite_topic:{lite_topic} success.")
        except Exception as e:
            logger.error(f"[{self.__consumer}] subscribe lite_topic:{lite_topic} raise exception, {e}.")
            raise e

    def unsubscribe_lite(self, lite_topic):
        """Unsubscribe from a lite topic.

        If the lite topic is not currently subscribed, this is a no-op.

        Args:
            lite_topic: The name of the lite topic to unsubscribe from.

        Raises:
            IllegalStateException: If consumer is not running.
        """

        if not self.__consumer.is_running:
            raise IllegalStateException("unable to remove lite subscription because consumer is not running")
        if lite_topic not in self.__lite_topics:
            return
        try:
            request = self.__remove_lite_subscription_req({lite_topic}, self.__consumer.consumer_group, self.__consumer.client_configuration)
            res = self.__consumer.rpc_client.sync_lite_subscription_async(
                self.__consumer.client_configuration.rpc_endpoints, request, metadata=self.__consumer.sign(), timeout=self.__consumer.client_configuration.request_timeout,
            ).result()
            self.__handle_lite_subscription_response(request, res, lite_topic)
            logger.info(f"[{self.__consumer}] unsubscribe lite_topic:{lite_topic} success.")
        except Exception as e:
            logger.error(f"[{self.__consumer}] unsubscribe lite_topic:{lite_topic} raise exception, {e}.")
            raise e

    def sync_all_lite_subscription(self):
        """Sync all currently subscribed lite topics to the server.

        Called periodically by the scheduler. Sends all lite topics in a single
        PARTIAL_ADD request and processes the server response.
        """

        try:
            request = self.__add_lite_subscription_req(self.__lite_topics, self.__consumer.consumer_group, self.__consumer.client_configuration, True)
            res = self.__consumer.rpc_client.sync_lite_subscription_async(
                self.__consumer.client_configuration.rpc_endpoints, request, metadata=self.__consumer.sign(), timeout=self.__consumer.client_configuration.request_timeout,
            ).result()
            self.__handle_lite_subscription_response(request, res, None)
            logger.info(f"{self.__consumer} sync all lite subscription to {self.__consumer.client_configuration.rpc_endpoints} success.")
        except Exception as e:
            logger.info(f"[{self.__consumer}] sync all lite subscription to {self.__consumer.client_configuration.rpc_endpoints} raise exception, {e}")

    def __add_lite_subscription_req(self, lite_topics, consumer_group, client_configuration, add_all=False):
        return self.__sync_lite_subscription_req(lite_topics, LiteSubscriptionAction.PARTIAL_ADD if not add_all else LiteSubscriptionAction.COMPLETE_ADD, consumer_group, client_configuration)

    def __remove_lite_subscription_req(self, lite_topics, consumer_group, client_configuration):
        return self.__sync_lite_subscription_req(lite_topics, LiteSubscriptionAction.PARTIAL_REMOVE, consumer_group, client_configuration)

    def __sync_lite_subscription_req(self, lite_topics, action, consumer_group, client_configuration):
        req = SyncLiteSubscriptionRequest()
        req.action = action
        req.topic.name = self.__bind_topic
        req.topic.resource_namespace = client_configuration.namespace
        req.group.name = consumer_group
        req.group.resource_namespace = client_configuration.namespace
        req.lite_topic_set.extend(lite_topics)
        return req

    def __subscribe_lite_pre_check(self, lite_topic):
        if lite_topic in self.__lite_topics:
            return
        if not lite_topic:
            raise IllegalStateException("liteTopic is blank.")
        if len(lite_topic) > self.__max_lite_topic_size.get():
            raise IllegalStateException(f"lite_topic: {lite_topic} length exceeded max length {self.__max_lite_topic_size.get()}.")
        if len(self.__lite_topics) + 1 > self.__lite_subscription_quota.get():
            raise LiteSubscriptionQuotaExceededException(f"Lite subscription exceed quota: {self.__lite_subscription_quota.get()} ", Code.LITE_SUBSCRIPTION_QUOTA_EXCEEDED)

    def __handle_lite_subscription_response(self, request, response, lite_topic):
        MessagingResultChecker.check(response.status)
        if response.status.code == Code.OK:
            if request.action == LiteSubscriptionAction.PARTIAL_ADD:
                if lite_topic:
                    self.__lite_topics.add(lite_topic)
            elif request.action == LiteSubscriptionAction.PARTIAL_REMOVE:
                self.__lite_topics.remove(lite_topic)

    @property
    def bind_topic(self):
        return self.__bind_topic

    @property
    def lite_topics(self):
        return self.__lite_topics.copy()

    @property
    def lite_subscription_quota(self):
        return self.__lite_subscription_quota.get()

    @property
    def max_lite_topic_size(self):
        return self.__max_lite_topic_size.get()
