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

from rocketmq.grpc_protocol import ClientType
from rocketmq.v5.client import ClientConfiguration
from rocketmq.v5.client.balancer import QueueSelector
from rocketmq.v5.consumer.consumer import Consumer
from rocketmq.v5.exception import (IllegalArgumentException,
                                   IllegalStateException)
from rocketmq.v5.log import logger
from rocketmq.v5.util import AtomicInteger, ConcurrentMap


class SimpleConsumer(Consumer):

    def __init__(
        self,
        client_configuration: ClientConfiguration,
        consumer_group,
        subscription: dict = None,
        await_duration=20,
        tls_enable=False
    ):
        if await_duration is None:
            raise IllegalArgumentException("awaitDuration should not be null")

        super().__init__(
            client_configuration,
            consumer_group,
            ClientType.SIMPLE_CONSUMER,
            subscription,
            tls_enable
        )

        self.__await_duration = await_duration  # long polling timeout, seconds
        # <String /* topic */, Int /* index */>
        self.__receive_queue_selectors = ConcurrentMap()
        self.__topic_index = AtomicInteger(0)
        self.__queue_index_lock = threading.Lock()

    def receive(self, max_message_num, invisible_duration):
        try:
            return self.__receive(max_message_num, invisible_duration)
        except Exception as e:
            raise e

    def receive_async(self, max_message_num, invisible_duration):
        try:
            return self.__receive(max_message_num, invisible_duration, sync=False)
        except Exception as e:
            raise e

    """ override """

    def shutdown(self):
        logger.info(f"begin to to shutdown {self}.")
        super().shutdown()
        logger.info(f"shutdown {self} success.")

    def _on_start(self):
        logger.info(f"{self} start success.")

    def _on_start_failure(self):
        logger.info(f"{self} start failed.")

    def _sync_setting_req(self, endpoints):
        req = super()._sync_setting_req(endpoints)
        req.settings.subscription.long_polling_timeout.seconds = self.__await_duration
        return req

    def _update_queue_selector(self, topic, topic_route):
        queue_selector = self.__receive_queue_selectors.get(topic)
        if queue_selector is None:
            return
        queue_selector.update(topic_route)

    """ private """

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
            queue_selector = self.__receive_queue_selectors.put_if_absent(
                topic, QueueSelector.simple_consumer_queue_selector(route)
            )
            return queue_selector.select_next_queue()
        except Exception as e:
            logger.error(f"simple consumer select topic queue raise exception: {e}")
            raise e

    def __receive(self, max_message_num, invisible_duration, sync=True):
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

    """ property """

    @property
    def await_duration(self):
        return self.__await_duration

    @await_duration.setter
    def await_duration(self, await_duration):
        self.__await_duration = await_duration
