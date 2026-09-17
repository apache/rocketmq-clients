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

from rocketmq.grpc_protocol import ClientType
from rocketmq.v5.client import (ClientConfiguration,
                                LiteTopicSubscriptionManager)
from rocketmq.v5.consumer.simple.simple_consumer import SimpleConsumer
from rocketmq.v5.exception import IllegalArgumentException
from rocketmq.v5.model import FilterExpression


class LiteSimpleConsumer(SimpleConsumer):
    """A pull-based consumer for lite topics bound to one physical topic.

    Lite topics share the route and message queues of the bound physical topic.
    Subscriptions are managed dynamically, while messages are fetched on demand
    through the inherited reception methods.
    """

    def __init__(
            self,
            bind_topic,
            client_configuration: ClientConfiguration,
            consumer_group,
            await_duration=20,
            tls_enable=False
    ):
        """Initialize the lite simple consumer.

        Args:
            bind_topic: The physical topic shared by all lite topics.
            client_configuration: Connection and authentication configuration.
            consumer_group: The consumer group name.
            await_duration: Long polling timeout in seconds (default: 20).
            tls_enable: Whether to enable TLS (default: False).

        Raises:
            IllegalArgumentException: If bind_topic is empty or await_duration is None.
        """
        if not bind_topic:
            raise IllegalArgumentException("bind_topic should not be null")

        super().__init__(
            client_configuration,
            consumer_group,
            {bind_topic: FilterExpression()},
            await_duration,
            ClientType.LITE_SIMPLE_CONSUMER,
            tls_enable
        )
        self.__subscription_manager = LiteTopicSubscriptionManager(self, bind_topic)

    def _on_start(self):
        """Start periodic lite subscription synchronization."""
        super()._on_start()
        self.__subscription_manager.start_scheduler(self._rpc_channel_io_loop())

    def reset_setting(self, settings):
        """Apply server settings for lite subscriptions.

        Args:
            settings: The :class:`Settings` protobuf returned by the server.
        """
        if not settings or not settings.subscription:
            return
        if settings.subscription.lite_subscription_quota is None or settings.subscription.max_lite_topic_size is None:
            return
        self.__subscription_manager.set_config(settings)
        super().reset_setting(settings)

    def shutdown(self):
        """Shutdown the consumer and stop lite subscription synchronization."""
        super().shutdown()
        self.__subscription_manager.stop_scheduler()

    def subscribe_lite(self, lite_topic):
        """Subscribe to a lite topic and synchronize it with the server.

        Args:
            lite_topic: The lite topic name to subscribe to.

        Raises:
            IllegalStateException: If the consumer is not running or the name is invalid.
            LiteSubscriptionQuotaExceededException: If the subscription quota is exceeded.
        """
        self.__subscription_manager.subscribe_lite(lite_topic)

    def unsubscribe_lite(self, lite_topic):
        """Unsubscribe from a lite topic.

        Args:
            lite_topic: The lite topic name to unsubscribe from.

        Raises:
            IllegalStateException: If the consumer is not running.
        """
        self.__subscription_manager.unsubscribe_lite(lite_topic)

    def subscribe_lite_all(self):
        """Subscribe to all lite topics under the bound parent topic."""
        self.__subscription_manager.sync_all_lite_subscription()

    def subscribe(self, topic, filter_expression=None):
        """Reject normal topic subscription for a lite consumer.

        Args:
            topic: The normal topic name.
            filter_expression: Optional message filter expression.

        Raises:
            NotImplementedError: Always, because the bind topic is fixed.
        """
        raise NotImplementedError("LiteSimpleConsumer does not support topic subscription.")

    def unsubscribe(self, topic):
        """Reject normal topic unsubscription for a lite consumer.

        Args:
            topic: The normal topic name.

        Raises:
            NotImplementedError: Always, because the bind topic is fixed.
        """
        raise NotImplementedError("LiteSimpleConsumer does not support topic unsubscription.")
