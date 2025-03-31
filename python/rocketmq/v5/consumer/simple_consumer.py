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

import asyncio
import functools
import threading
from concurrent.futures import Future

from rocketmq.grpc_protocol import (AckMessageEntry, AckMessageRequest,
                                    ChangeInvisibleDurationRequest, ClientType,
                                    HeartbeatRequest,
                                    NotifyClientTerminationRequest,
                                    ReceiveMessageRequest, Settings,
                                    Subscription, TelemetryCommand)
from rocketmq.v5.client import Client, ClientConfiguration
from rocketmq.v5.client.balancer import QueueSelector
from rocketmq.v5.exception import (IllegalArgumentException,
                                   IllegalStateException)
from rocketmq.v5.log import logger
from rocketmq.v5.model import CallbackResult, FilterExpression, Message
from rocketmq.v5.util import (AtomicInteger, ConcurrentMap,
                              MessagingResultChecker, Misc)


class SimpleConsumer(Client):

    def __init__(
        self,
        client_configuration: ClientConfiguration,
        consumer_group,
        subscription: dict = None,
        await_duration=20,
    ):
        if consumer_group is None or consumer_group.strip() == "":
            raise IllegalArgumentException("consumerGroup should not be null")
        if Misc.is_valid_consumer_group(consumer_group) is False:
            raise IllegalArgumentException(
                f"consumerGroup does not match the regex [regex={Misc.CONSUMER_GROUP_PATTERN}]"
            )
        if await_duration is None:
            raise IllegalArgumentException("awaitDuration should not be null")

        super().__init__(
            client_configuration,
            None if subscription is None else subscription.keys(),
            ClientType.SIMPLE_CONSUMER,
        )
        self.__consumer_group = consumer_group
        self.__await_duration = await_duration  # long polling timeout, seconds
        # <String /* topic */, FilterExpression>
        self.__subscriptions = ConcurrentMap()
        if subscription is not None:
            self.__subscriptions.update(subscription)
        # <String /* topic */, Int /* index */>
        self.__receive_queue_selectors = ConcurrentMap()
        self.__topic_index = AtomicInteger(0)
        self.__queue_index_lock = threading.Lock()

    def __str__(self):
        return f"{ClientType.Name(self.client_type)}:{self.consumer_group}, client_id:{self.client_id}"

    def subscribe(self, topic, filter_expression: FilterExpression = None):
        if self.is_running is False:
            raise IllegalStateException(
                "unable to add subscription because simple consumer is not running"
            )

        try:
            if not self.__subscriptions.contains(topic):
                self._retrieve_topic_route_data(topic)
            self.__subscriptions.put(
                topic,
                (
                    filter_expression
                    if filter_expression is not None
                    else FilterExpression()
                ),
            )
        except Exception as e:
            logger.error(f"subscribe raise exception: {e}")
            raise e

    def unsubscribe(self, topic):
        if self.is_running is False:
            raise IllegalStateException(
                "unable to remove subscription because simple consumer is not running"
            )

        if topic in self.__subscriptions:
            self.__subscriptions.remove(topic)
            self._remove_unused_topic_route_data(topic)

    def receive(self, max_message_num, invisible_duration):
        try:
            future, queue = self.__receive(max_message_num, invisible_duration)
            read_future = asyncio.run_coroutine_threadsafe(
                self.__receive_message_response(future.result()),
                self._rpc_channel_io_loop(),
            )
            return self.__handle_receive_message_response(read_future.result(), queue)
        except Exception as e:
            raise e

    def receive_async(self, max_message_num, invisible_duration):
        try:
            future, queue = self.__receive(max_message_num, invisible_duration)
            read_future = asyncio.run_coroutine_threadsafe(
                self.__receive_message_response(future.result()),
                self._rpc_channel_io_loop(),
            )
            ret_future = Future()
            handle_send_receipt_callback = functools.partial(
                self.__receive_message_callback, ret_future=ret_future, queue=queue
            )
            read_future.add_done_callback(handle_send_receipt_callback)
            return ret_future
        except Exception as e:
            raise e

    def ack(self, message: Message):
        try:
            future = self.__ack(message)
            self.__handle_ack_result(future)
        except Exception as e:
            raise e

    def ack_async(self, message: Message):
        try:
            future = self.__ack(message)
            ret_future = Future()
            ack_callback = functools.partial(
                self.__handle_ack_result, ret_future=ret_future
            )
            future.add_done_callback(ack_callback)
            return ret_future
        except Exception as e:
            raise e

    def change_invisible_duration(self, message: Message, invisible_duration):
        try:
            future = self.__change_invisible_duration(message, invisible_duration)
            self.__handle_change_invisible_result(future, message)
        except Exception as e:
            raise e

    def change_invisible_duration_async(self, message: Message, invisible_duration):
        try:
            future = self.__change_invisible_duration(message, invisible_duration)
            ret_future = Future()
            change_invisible_callback = functools.partial(
                self.__handle_change_invisible_result, message=message, ret_future=ret_future
            )
            future.add_done_callback(change_invisible_callback)
            return ret_future
        except Exception as e:
            raise e

    """ override """

    def _start_success(self):
        logger.info(f"{self.__str__()} start success.")

    def _start_failure(self):
        logger.info(f"{self.__str__()} start failed.")

    def _sync_setting_req(self, endpoints):
        subscription = Subscription()
        subscription.group.name = self.__consumer_group
        subscription.group.resource_namespace = self.client_configuration.namespace
        subscription.fifo = False
        subscription.long_polling_timeout.seconds = self.__await_duration
        items = self.__subscriptions.items()
        for topic, expression in items:
            sub_entry = subscription.subscriptions.add()
            sub_entry.topic.name = topic
            sub_entry.topic.resource_namespace = self.client_configuration.namespace
            sub_entry.expression.type = expression.filter_type
            sub_entry.expression.expression = expression.expression

        settings = Settings()
        settings.client_type = self.client_type
        settings.access_point.CopyFrom(endpoints.endpoints)
        settings.request_timeout.seconds = self.client_configuration.request_timeout
        settings.subscription.CopyFrom(subscription)
        settings.user_agent.language = 6
        settings.user_agent.version = Misc.sdk_version()
        settings.user_agent.platform = Misc.get_os_description()
        settings.user_agent.hostname = Misc.get_local_ip()
        settings.metric.on = False

        cmd = TelemetryCommand()
        cmd.settings.CopyFrom(settings)
        return cmd

    def _heartbeat_req(self):
        req = HeartbeatRequest()
        req.client_type = self.client_type
        req.group.name = self.__consumer_group
        req.group.resource_namespace = self.client_configuration.namespace
        return req

    def _notify_client_termination_req(self):
        req = NotifyClientTerminationRequest()
        req.group.resource_namespace = self.client_configuration.namespace
        req.group.name = self.__consumer_group
        return req

    def _update_queue_selector(self, topic, topic_route):
        queue_selector = self.__receive_queue_selectors.get(topic)
        if queue_selector is None:
            return
        queue_selector.update(topic_route)

    def shutdown(self):
        logger.info(f"begin to to shutdown {self.__str__()}.")
        super().shutdown()
        logger.info(f"shutdown {self.__str__()} success.")

    """ private """

    # receive message

    def __select_topic_for_receive(self):
        try:
            # select the next topic for receive
            mod_index = self.__topic_index.get_and_increment() % len(
                self.__subscriptions.keys()
            )
            return list(self.__subscriptions.keys())[mod_index]
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

    def __receive_pre_check(self, max_message_num):
        if self.is_running is False:
            raise IllegalStateException("consumer is not running now.")
        if len(self.__subscriptions.keys()) == 0:
            raise IllegalArgumentException("There is no topic to receive message")
        if max_message_num <= 0:
            raise IllegalArgumentException("max_message_num must be greater than 0")

    def __receive_req(self, topic, queue, max_message_num, invisible_duration):
        filter_expression = self.__subscriptions.get(topic)
        req = ReceiveMessageRequest()
        req.group.name = self.__consumer_group
        req.group.resource_namespace = self.client_configuration.namespace
        req.message_queue.CopyFrom(queue.message_queue0())
        req.filter_expression.type = filter_expression.filter_type
        req.filter_expression.expression = filter_expression.expression
        req.batch_size = max_message_num
        req.invisible_duration.seconds = invisible_duration
        req.long_polling_timeout.seconds = self.__await_duration
        req.auto_renew = False
        return req

    def __receive(self, max_message_num, invisible_duration):
        if self.is_running is False:
            raise IllegalStateException(
                "unable to receive messages because simple consumer is not running"
            )
        try:
            self.__receive_pre_check(max_message_num)
            topic = self.__select_topic_for_receive()
            queue = self.__select_topic_queue(topic)
            req = self.__receive_req(topic, queue, max_message_num, invisible_duration)
            timeout = self.client_configuration.request_timeout + self.__await_duration
            return self.rpc_client.receive_message_async(
                queue.endpoints, req, metadata=self._sign(), timeout=timeout
            ), queue
        except Exception as e:
            raise e

    def __receive_message_callback(self, future, ret_future, queue):
        try:
            responses = future.result()
            messages = self.__handle_receive_message_response(responses, queue)
            self._submit_callback(
                CallbackResult.async_receive_callback_result(ret_future, messages)
            )
        except Exception as e:
            self._submit_callback(
                CallbackResult.async_receive_callback_result(ret_future, e, False)
            )

    async def __receive_message_response(self, unary_stream_call):
        try:
            responses = list()
            async for res in unary_stream_call:
                if res.HasField("message") or res.HasField("status"):
                    logger.debug(
                        f"consumer:{self.__consumer_group} receive response: {res}"
                    )
                    responses.append(res)
            return responses
        except Exception as e:
            logger.error(
                f"consumer:{self.__consumer_group} receive message exception: {e}"
            )
            raise e

    def __handle_receive_message_response(self, responses, queue):
        messages = list()
        status = None

        for res in responses:
            if res.HasField("status"):
                logger.debug(
                    f"simple_consumer[{self.__consumer_group}] receive_message, code:{res.status.code}, message:{res.status.message}."
                )
                status = res.status
            elif res.HasField("message"):
                msg = Message().fromProtobuf(res.message)
                msg.endpoints = queue.endpoints
                messages.append(msg)

        MessagingResultChecker.check(status)
        return messages

    # ack message

    def __ack_req(self, message: Message):
        req = AckMessageRequest()
        req.group.name = self.__consumer_group
        req.group.resource_namespace = self.client_configuration.namespace
        req.topic.name = message.topic
        req.topic.resource_namespace = self.client_configuration.namespace

        msg_entry = AckMessageEntry()
        msg_entry.message_id = message.message_id
        msg_entry.receipt_handle = message.receipt_handle
        req.entries.append(msg_entry)
        return req

    def __ack(self, message: Message):
        if self.is_running is False:
            raise IllegalStateException(
                "unable to ack message because simple consumer is not running"
            )
        try:
            return self.rpc_client.ack_message_async(
                message.endpoints,
                self.__ack_req(message),
                metadata=self._sign(),
                timeout=self.client_configuration.request_timeout,
            )
        except Exception as e:
            raise e

    def __handle_ack_result(self, future, ret_future=None):
        try:
            res = future.result()
            logger.debug(
                f"consumer[{self.__consumer_group}] ack response, {res.status}"
            )
            MessagingResultChecker.check(res.status)
            if ret_future is not None:
                self._submit_callback(
                    CallbackResult.async_ack_callback_result(ret_future, None)
                )
        except Exception as e:
            if ret_future is None:
                raise e
            else:
                self._submit_callback(
                    CallbackResult.async_ack_callback_result(ret_future, e, False)
                )

    # change_invisible

    def __change_invisible_req(self, message: Message, invisible_duration):
        req = ChangeInvisibleDurationRequest()
        req.topic.name = message.topic
        req.topic.resource_namespace = self.client_configuration.namespace
        req.group.name = self.consumer_group
        req.group.resource_namespace = self.client_configuration.namespace
        req.receipt_handle = message.receipt_handle
        req.invisible_duration.seconds = invisible_duration
        req.message_id = message.message_id
        return req

    def __change_invisible_duration(self, message: Message, invisible_duration):
        if self.is_running is False:
            raise IllegalStateException(
                "unable to change invisible duration because simple consumer is not running"
            )
        try:
            return self.rpc_client.change_invisible_duration_async(
                message.endpoints,
                self.__change_invisible_req(message, invisible_duration),
                metadata=self._sign(),
                timeout=self.client_configuration.request_timeout,
            )
        except Exception as e:
            raise e

    def __handle_change_invisible_result(self, future, message, ret_future=None):
        try:
            res = future.result()
            logger.debug(
                f"consumer[{self.__consumer_group}] change invisible response, {res.status}"
            )
            message.receipt_handle = res.receipt_handle
            MessagingResultChecker.check(res.status)
            if ret_future is not None:
                self._submit_callback(
                    CallbackResult.async_change_invisible_duration_callback_result(
                        ret_future, None
                    )
                )
        except Exception as e:
            if ret_future is None:
                raise e
            else:
                self._submit_callback(
                    CallbackResult.async_change_invisible_duration_callback_result(
                        ret_future, e, False
                    )
                )

    """ property """

    @property
    def consumer_group(self):
        return self.__consumer_group

    @property
    def await_duration(self):
        return self.__await_duration

    @await_duration.setter
    def await_duration(self, await_duration):
        self.__await_duration = await_duration
