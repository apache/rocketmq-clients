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
import os
from asyncio import Future
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from rocketmq import ClientConfiguration
from rocketmq.grpc_protocol import (AckMessageEntry, AckMessageRequest,
                                    ChangeInvisibleDurationRequest, ClientType,
                                    HeartbeatRequest,
                                    NotifyClientTerminationRequest,
                                    ReceiveMessageRequest, Settings,
                                    Subscription, TelemetryCommand)
from rocketmq.v5.client import Client
from rocketmq.v5.exception import (IllegalArgumentException,
                                   IllegalStateException)
from rocketmq.v5.log import logger
from rocketmq.v5.model import CallbackResult, FilterExpression, Message
from rocketmq.v5.util import (SDK_VERSION, ConcurrentMap,
                              MessagingResultChecker, Misc)


class Consumer(Client):
    """Base class for all consumer types (Push, Simple).

    Manages topic subscriptions, message reception, acknowledgment,
    and invisible duration control. Subclasses implement specific
    consumption strategies (push callbacks vs pull-based).
    """

    def __init__(
        self,
        client_configuration: ClientConfiguration,
        consumer_group,
        client_type,
        subscription: Optional[dict] = None,
        tls_enable=False
    ):
        """Initialize the consumer.

        Args:
            client_configuration: Connection and authentication configuration.
            consumer_group: The consumer group name for consumption.
            client_type: The :class:`ClientType` (PUSH_CONSUMER, SIMPLE_CONSUMER, etc.).
            subscription: Optional dict of ``{topic: FilterExpression}`` for initial subscriptions.
            tls_enable: Whether to enable TLS for gRPC connections.

        Raises:
            IllegalArgumentException: If consumer_group is blank or invalid.
        """
        if consumer_group is None or consumer_group.strip() == "":
            raise IllegalArgumentException("consumerGroup should not be null")
        if Misc.is_valid_consumer_group(consumer_group) is False:
            raise IllegalArgumentException(
                f"consumerGroup does not match the regex [regex={Misc.CONSUMER_GROUP_PATTERN}]"
            )
        super().__init__(
            client_configuration,
            subscription.keys() if subscription else None,
            client_type,
            tls_enable
        )
        self._consumer_group = consumer_group
        # <String /* topic */, FilterExpression>
        self._subscriptions = ConcurrentMap()
        if subscription:
            self._subscriptions.update(subscription)
        self.__message_decode_executor = ThreadPoolExecutor(
            max_workers=os.cpu_count(),
            thread_name_prefix=f"{self.client_id}_message_decode_thread",
        )

    def __str__(self):
        return f"{ClientType.Name(self.client_type)}:{self.consumer_group}, client_id:{self.client_id}"

    def subscribe(self, topic, filter_expression: Optional[FilterExpression] = None):
        """Subscribe to a topic with an optional filter expression.

        The consumer will receive messages from all subscribed topics that
        match the filter (tag-based or SQL92). If the topic route data is
        not cached, it will be fetched from the server.

        Args:
            topic: The topic name to subscribe to.
            filter_expression: Optional :class:`FilterExpression` for tag or SQL92 filtering.

        Raises:
            IllegalStateException: If consumer is not running or topic name is invalid.
        """
        if not self.is_running:
            raise IllegalStateException(
                "unable to add subscription because consumer is not running"
            )
        if Misc.is_valid_topic(topic) is False:
            raise IllegalStateException(
                "unable to add subscription because topic name is invalid"
            )
        try:
            if not self._subscriptions.contains(topic):
                self._retrieve_topic_route_data(topic)
            self._subscriptions.put(
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
        """Unsubscribe from a topic.

        Removes the topic from the subscription list and cleans up unused
        route data and process queues.

        Args:
            topic: The topic name to unsubscribe from.

        Raises:
            IllegalStateException: If consumer is not running.
        """
        if not self.is_running:
            raise IllegalStateException(
                "unable to remove subscription because consumer is not running"
            )

        if self._subscriptions.contains(topic):
            self._subscriptions.remove(topic)
            self._remove_unused_topic_route_data(topic)

    def is_lite_consumer(self):
        """Check if this consumer is a lite consumer (LitePushConsumer or LiteSimpleConsumer).

        Returns:
            True if the consumer type is LITE_PUSH_CONSUMER or LITE_SIMPLE_CONSUMER.
        """
        return self.client_type == ClientType.LITE_PUSH_CONSUMER or self.client_type == ClientType.LITE_SIMPLE_CONSUMER

    def sync_setting_req(self, endpoints):
        """Build the settings request for initial sync with the server.

        Constructs a TelemetryCommand containing subscription information
        (consumer group, topics, filter expressions) and client info.

        Args:
            endpoints: The broker endpoints to sync settings with.

        Returns:
            A :class:`TelemetryCommand` with the consumer's settings.
        """
        subscription = Subscription()
        subscription.group.name = self._consumer_group
        subscription.group.resource_namespace = self.client_configuration.namespace
        items = self._subscriptions.items()
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
        settings.user_agent.version = SDK_VERSION
        settings.user_agent.platform = Misc.get_os_description()
        settings.user_agent.hostname = Misc.get_local_ip()
        settings.metric.on = False

        cmd = TelemetryCommand()
        cmd.settings.CopyFrom(settings)
        return cmd

    def _ack(self, message: Message):

        future = self.__ack(message)
        self.__handle_ack_result(future)

    def _ack_async(self, message: Message):

        future = self.__ack(message)
        ret_future = Future()
        ack_callback = functools.partial(
            self.__handle_ack_result, ret_future=ret_future
        )
        future.add_done_callback(ack_callback)
        return ret_future

    def _change_invisible_duration(self, message: Message, invisible_duration):

        future = self.__change_invisible_duration(message, invisible_duration)
        self.__handle_change_invisible_result(future, message)

    def _change_invisible_duration_async(self, message: Message, invisible_duration):

        future = self.__change_invisible_duration(message, invisible_duration)
        ret_future = Future()
        change_invisible_callback = functools.partial(
            self.__handle_change_invisible_result, message=message, ret_future=ret_future
        )
        future.add_done_callback(change_invisible_callback)
        return ret_future

    def _receive(self, queue, req, timeout):
        future = asyncio.run_coroutine_threadsafe(
            self._receive_coroutine(queue, req, timeout),
            self._rpc_channel_io_loop(),
        )
        return future.result()

    def _receive_async(self, queue, req, timeout, ret_future):
        receive_future = asyncio.run_coroutine_threadsafe(
            self._receive_coroutine(queue, req, timeout),
            self._rpc_channel_io_loop(),
        )
        receive_future.add_done_callback(
            functools.partial(
                self.__receive_message_callback,
                ret_future=ret_future,
            )
        )
        return ret_future

    async def _receive_coroutine(self, queue, req, timeout):
        call = None
        try:
            call = await self.rpc_client.receive_message(
                queue.endpoints,
                req,
                metadata=self.sign(),
                timeout=timeout,
            )
            responses = await self.__receive_message_response(call)
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                self.__message_decode_executor,
                self.__handle_receive_message_response,
                responses,
                queue,
            )
        except asyncio.CancelledError:
            if call is not None:
                call.cancel()
            raise

    def _receive_req(self, topic, queue, max_message_num, auto_renew, invisible_duration=None, long_polling_timeout=None, attempt_id=None):
        filter_expression = self._subscriptions.get(topic)
        req = ReceiveMessageRequest()
        req.group.name = self._consumer_group
        req.group.resource_namespace = self.client_configuration.namespace
        req.message_queue.CopyFrom(queue.message_queue0())
        if filter_expression:
            req.filter_expression.type = filter_expression.filter_type
            req.filter_expression.expression = filter_expression.expression
        req.batch_size = max_message_num
        if invisible_duration:
            req.invisible_duration.seconds = invisible_duration
        if long_polling_timeout:
            req.long_polling_timeout.seconds = long_polling_timeout
        if attempt_id:
            req.attempt_id = attempt_id
        req.auto_renew = auto_renew
        return req

    def _heartbeat_req(self):
        req = HeartbeatRequest()
        req.client_type = self.client_type
        req.group.name = self._consumer_group
        req.group.resource_namespace = self.client_configuration.namespace
        return req

    def _notify_client_termination_req(self):
        req = NotifyClientTerminationRequest()
        req.group.resource_namespace = self.client_configuration.namespace
        req.group.name = self._consumer_group
        return req

    async def __receive_message_response(self, unary_stream_call):
        try:
            responses = list()
            async for res in unary_stream_call:
                if res.HasField("message") or res.HasField("status") or res.HasField("delivery_timestamp"):
                    logger.debug(
                        f"consumer:{self._consumer_group} receive response: {res}"
                    )
                    responses.append(res)
            return responses
        except Exception as e:
            logger.error(
                f"consumer:{self._consumer_group} receive message exception: {e}"
            )
            raise e

    def __handle_receive_message_response(self, responses, queue):
        messages = list()
        status = None
        transport_delivery_timestamp = None

        for res in responses:
            if res.HasField("status"):
                logger.debug(
                    f"consumer:{self._consumer_group} receive_message, code:{res.status.code}, message:{res.status.message}."
                )
                status = res.status
            elif res.HasField("message"):
                msg = Message().fromProtobuf(res.message)
                msg.endpoints = queue.endpoints
                messages.append(msg)
            elif res.HasField("delivery_timestamp"):
                transport_delivery_timestamp = Misc.to_mills(res.delivery_timestamp)
        if not status:
            logger.error("[BUG] handle received message occur error, status is None.")
            return messages
        MessagingResultChecker.check(status)
        if len(messages) > 0 and transport_delivery_timestamp:
            for msg in messages:
                msg.transport_delivery_timestamp = transport_delivery_timestamp
        return messages

    def __receive_message_callback(self, future, ret_future):
        try:
            messages = future.result()
            result = CallbackResult.async_receive_callback_result(
                ret_future, messages
            )
        except Exception as e:
            result = CallbackResult.async_receive_callback_result(
                ret_future, e, False
            )

        self._submit_callback(result)

    def __ack_req(self, message: Message):
        req = AckMessageRequest()
        req.group.name = self._consumer_group
        req.group.resource_namespace = self.client_configuration.namespace
        req.topic.name = message.topic
        req.topic.resource_namespace = self.client_configuration.namespace

        msg_entry = AckMessageEntry()
        msg_entry.message_id = message.message_id
        msg_entry.receipt_handle = message.receipt_handle
        if self.is_lite_consumer():
            msg_entry.lite_topic = message.lite_topic
        req.entries.append(msg_entry)
        return req

    def __ack(self, message: Message):
        if not self.is_running:
            raise IllegalStateException(
                "unable to ack message because consumer is not running"
            )

        return self.rpc_client.ack_message_async(
            message.endpoints,
            self.__ack_req(message),
            metadata=self.sign(),
            timeout=self.client_configuration.request_timeout,
        )

    def __handle_ack_result(self, future, ret_future=None):
        try:
            res = future.result()
            logger.debug(
                f"consumer:{self._consumer_group} ack response, {res.status}"
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

    def __change_invisible_req(self, message: Message, invisible_duration):
        req = ChangeInvisibleDurationRequest()
        req.topic.name = message.topic
        req.topic.resource_namespace = self.client_configuration.namespace
        req.group.name = self._consumer_group
        req.group.resource_namespace = self.client_configuration.namespace
        req.receipt_handle = message.receipt_handle
        req.invisible_duration.seconds = invisible_duration
        req.message_id = message.message_id
        if self.is_lite_consumer():
            req.lite_topic = message.lite_topic
        return req

    def __change_invisible_duration(self, message: Message, invisible_duration):
        if not self.is_running:
            raise IllegalStateException(
                "unable to change invisible duration because consumer is not running"
            )

        return self.rpc_client.change_invisible_duration_async(
            message.endpoints,
            self.__change_invisible_req(message, invisible_duration),
            metadata=self.sign(),
            timeout=self.client_configuration.request_timeout,
        )

    def __handle_change_invisible_result(self, future, message, ret_future=None):
        try:
            res = future.result()
            logger.debug(
                f"consumer:{self._consumer_group} change invisible response, {res.status}"
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

    @property
    def consumer_group(self):
        """The consumer group name this consumer belongs to."""
        return self._consumer_group
