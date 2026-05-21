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

from rocketmq.grpc_protocol import (ClientType, Code, LiteSubscriptionAction,
                                    SyncLiteSubscriptionRequest)
from rocketmq.v5.client import ClientConfiguration, ClientScheduler
from rocketmq.v5.exception import (IllegalStateException,
                                   LiteSubscriptionQuotaExceededException)
from rocketmq.v5.log import logger
from rocketmq.v5.model.filter_expression import FilterExpression
from rocketmq.v5.util import AtomicInteger, MessagingResultChecker

from .message_listener import MessageListener
from .push_consumer import PushConsumer


class LitePushConsumer(PushConsumer):

    def __init__(
        self,
        client_configuration: ClientConfiguration,
        consumer_group,
        bind_topic,
        message_listener: MessageListener = None,
        max_cache_message_count=1024,
        max_cache_message_size=64 * 1024 * 1024,  # in bytes, 64MB default
        consumption_thread_count=20,
        tls_enable=False,
    ):
        if not bind_topic:
            raise Exception("bindTopic has not been set yet.")

        super().__init__(
            client_configuration,
            consumer_group,
            message_listener,
            {bind_topic: FilterExpression()},
            max_cache_message_count,
            max_cache_message_size,
            consumption_thread_count,
            tls_enable,
            ClientType.LITE_PUSH_CONSUMER
        )
        self.__bind_topic = bind_topic
        self.__lite_topics = set()
        self.__lite_subscription_quota = AtomicInteger(0)
        self.__max_lite_topic_size = 64

    """ override """

    def _on_start(self):
        super()._on_start()
        try:
            self._sync_all_lite_subscription_scheduler = ClientScheduler(f"{self.client_id}_sync_all_lite_subscription_scheduler_thread", self.__sync_all_lite_subscription, 30, 30, self._rpc_channel_io_loop())
            self._sync_all_lite_subscription_scheduler.start_scheduler()
            logger.info("start sync all lite subscription scheduler success.")
        except Exception as e:
            logger.error(f"{self} on start error, {e}")
            raise e

    def reset_setting(self, settings):
        if not settings or not settings.subscription:
            return
        if not settings.subscription.lite_subscription_quota or not settings.subscription.max_lite_topic_size:
            return
        self.__lite_subscription_quota = AtomicInteger(settings.subscription.lite_subscription_quota)
        self.__max_lite_topic_size = settings.subscription.max_lite_topic_size
        super().reset_setting(settings)

    """ public """

    def subscribe_lite(self, lite_topic):
        if not self.is_running:
            raise IllegalStateException("unable to add lite subscription because consumer is not running")
        if lite_topic in self.__lite_topics:
            return
        if not lite_topic:
            raise IllegalStateException("liteTopic is blank.")
        if len(lite_topic) > self.__max_lite_topic_size:
            raise IllegalStateException(f"lite_topic: {lite_topic} length exceeded max length {self.__max_lite_topic_size}.")
        if len(self.__lite_topics) + 1 > self.__lite_subscription_quota.get():
            raise LiteSubscriptionQuotaExceededException(f"Lite subscription exceed quota: {self.__lite_subscription_quota.get()} ", Code.LITE_SUBSCRIPTION_QUOTA_EXCEEDED)
        try:
            res = self.rpc_client.sync_lite_subscription_async(
                self.client_configuration.rpc_endpoints,
                self.__sync_lite_subscription_req({lite_topic}, LiteSubscriptionAction.PARTIAL_ADD),
                metadata=self._sign(),
                timeout=self.client_configuration.request_timeout,
            ).result()
            MessagingResultChecker.check(res.status)
            if res.status.code == Code.OK:
                self.__lite_topics.add(lite_topic)
                logger.info(f"[{self}] subscribe lite_topic:{lite_topic} success.")
        except Exception as e:
            logger.info(f"[{self}] subscribe lite_topic:{lite_topic} raise exception, {e}.")

    def unsubscribe_lite(self, lite_topic):
        if not self.is_running:
            raise IllegalStateException("unable to remove lite subscription because consumer is not running")
        if lite_topic not in self.__lite_topics:
            return
        try:
            res = self.rpc_client.sync_lite_subscription_async(
                self.client_configuration.rpc_endpoints,
                self.__sync_lite_subscription_req({lite_topic}, LiteSubscriptionAction.PARTIAL_REMOVE),
                metadata=self._sign(),
                timeout=self.client_configuration.request_timeout,
            ).result()
            MessagingResultChecker.check(res.status)
            if res.status.code == Code.OK:
                self.__lite_topics.remove(lite_topic)
                logger.info(f"[{self}] unsubscribe lite_topic:{lite_topic} success.")
        except Exception as e:
            logger.info(f"[{self}] unsubscribe lite_topic:{lite_topic} raise exception, {e}.")

    def subscribe(self, topic, filter_expression: FilterExpression = None):
        raise NotImplementedError("LitePushConsumer does not support topic subscription.")

    def unsubscribe(self, topic):
        raise NotImplementedError("LitePushConsumer does not support topic unsubscription.")

    """ private """

    def __sync_all_lite_subscription(self):
        if not self.__lite_topics:
            return
        try:
            res = self.rpc_client.sync_lite_subscription_async(
                self.client_configuration.rpc_endpoints,
                self.__sync_lite_subscription_req(self.__lite_topics.copy(), LiteSubscriptionAction.COMPLETE_ADD),
                metadata=self._sign(),
                timeout=self.client_configuration.request_timeout,
            ).result()
            MessagingResultChecker.check(res.status)
            if res.status.code == Code.OK:
                logger.info(f"{self} sync all lite subscription to {self.client_configuration.rpc_endpoints} success.")
        except Exception as e:
            logger.info(f"[{self}] sync all lite subscription to {self.client_configuration.rpc_endpoints} raise exception, {e}")

    def __sync_lite_subscription_req(self, lite_topics, action: LiteSubscriptionAction):
        req = SyncLiteSubscriptionRequest()
        req.action = action
        req.topic.name = self.__bind_topic
        req.topic.resource_namespace = self.client_configuration.namespace
        req.group.name = self._consumer_group
        req.group.resource_namespace = self.client_configuration.namespace
        req.lite_topic_set.extend(lite_topics)
        return req
