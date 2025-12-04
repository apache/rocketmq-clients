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
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from rocketmq.grpc_protocol import (ClientType, Code,
                                    ForwardMessageToDeadLetterQueueRequest,
                                    QueryAssignmentRequest)
from rocketmq.v5.client import ClientConfiguration, ClientScheduler
from rocketmq.v5.consumer.consumer import Consumer
from rocketmq.v5.consumer.consumption import Consumption
from rocketmq.v5.consumer.fifo_consumption import FifoConsumption
from rocketmq.v5.consumer.message_listener import (ConsumeResult,
                                                   MessageListener)
from rocketmq.v5.exception import BadRequestException, IllegalStateException
from rocketmq.v5.log import logger
from rocketmq.v5.model.assignment import Assignments
from rocketmq.v5.model.process_queue import ProcessQueue
from rocketmq.v5.model.retry_policy import CustomizedBackoffRetryPolicy
from rocketmq.v5.util import ConcurrentMap, MessagingResultChecker


class PushConsumer(Consumer):

    ACK_MESSAGE_FAILURE_BACKOFF_DELAY = 1
    RECEIVE_RETRY_DELAY = 1

    def __init__(
        self,
        client_configuration: ClientConfiguration,
        consumer_group,
        message_listener: MessageListener = None,
        subscription: dict = None,
        max_cache_message_count=1024,
        max_cache_message_size=64 * 1024 * 1024,  # in bytes, 64MB default
        consumption_thread_count=20,
        tls_enable=False
    ):
        super().__init__(
            client_configuration,
            consumer_group,
            ClientType.PUSH_CONSUMER,
            subscription,
            tls_enable
        )
        self.__message_listener = message_listener
        self.__consumption = None
        self.__max_cache_message_count = max_cache_message_count
        self.__max_cache_message_size = max_cache_message_size
        self.__consumption_thread_count = consumption_thread_count
        # <message_queue, process_queue>
        self.__process_queues = ConcurrentMap()
        # <String /* topic */, assignments>
        self.__assignments = ConcurrentMap()
        self.__fifo = False
        self.__scan_assignment_scheduler = None
        self.__receive_batch_size = 32
        self.__long_polling_timeout = 30  # seconds

    def set_listener(self, message_listener: MessageListener):
        self.__message_listener = message_listener

    """ override """

    def shutdown(self):
        logger.info(f"begin to to shutdown {self}.")
        super().shutdown()
        self.__scan_assignment_scheduler.stop_scheduler()
        logger.info(f"shutdown {self} success.")

    def reset_setting(self, settings):
        self.__long_polling_timeout = settings.subscription.long_polling_timeout.seconds
        self.__configure_consumer_consumption(settings)

    def reset_metric(self, metric):
        super().reset_metric(metric)
        self.__register_process_queues_gauges()

    def _sync_setting_req(self, endpoints):
        req = super()._sync_setting_req(endpoints)
        req.settings.subscription.long_polling_timeout.seconds = self.__long_polling_timeout
        return req

    def _pre_start(self):
        if not self.__message_listener:
            logger.error("message listener has not been set yet")
            raise IllegalStateException(f"{self}'s message listener has not been set yet.")

    def _on_start(self):
        try:
            self.__start_async_executor()
            self.__scan_assignment_scheduler = ClientScheduler(f"{self.client_id}_scan_assignment_schedule_thread", self.__scan_assignment, 1, 5, self._rpc_channel_io_loop())
            self.__scan_assignment_scheduler.start_scheduler()
            logger.info(f"{self} start success.")
        except Exception as e:
            logger.error(f"{self} on start error, {e}")
            raise e

    def _on_start_failure(self):
        logger.info(f"{self} start failed.")

    """ private """

    def __start_async_executor(self):
        try:
            self.__receive_message_executor = ThreadPoolExecutor(thread_name_prefix=f"{self.client_id}_receive_message_thread")
            logger.info(f"{self} new receive message executor success.")
            self.__ack_or_nack_result_executor = ThreadPoolExecutor(thread_name_prefix=f"{self.client_id}_ack_or_nack_result_thread")
            logger.info(f"{self} new message consumption result executor success.")
        except Exception as e:
            logger.error(f"{self} start async executor raise exception, {e}")
            raise e

    def __configure_consumer_consumption(self, settings):
        if settings.backoff_policy.WhichOneof("strategy") == "customized_backoff":
            backoff_policy = CustomizedBackoffRetryPolicy(settings.backoff_policy)
        else:
            backoff_policy = CustomizedBackoffRetryPolicy(None)

        if not self.__consumption:
            self.__fifo = settings.subscription.fifo
            if not self.__fifo:
                self.__consumption = Consumption(self.__message_listener, self.__consumption_thread_count,
                                                 self.__on_consumption_result,
                                                 self.client_id, backoff_policy)
            else:
                self.__consumption = FifoConsumption(self.__message_listener, self.__consumption_thread_count,
                                                     self.__on_fifo_consumption_result,
                                                     self.client_id, backoff_policy)
        else:
            self.__consumption.backoff_policy = backoff_policy

    # assignment #

    def __scan_assignment(self):
        if not self.__consumption:
            return
        for topic in self._subscriptions.keys():
            try:
                topic_route = self._retrieve_topic_route_data(topic)
                size = len(topic_route.message_queues)
                if size > 0:
                    queue = topic_route.message_queues[random.randint(0, size - 1)]
                    req = self.__query_assignment_req(topic, queue)
                    future = self.rpc_client.query_assignment_async(
                        queue.endpoints, req, metadata=self._sign(), timeout=self.client_configuration.request_timeout
                    )
                    future.add_done_callback(functools.partial(self.__handle_assignment_result, topic=topic))
            except Exception as e:
                logger.error(f"scan topic: {topic} assignment raise exception. {e}")

    def __handle_assignment_result(self, future, topic):
        try:
            # in io_loop thread, don't do any network io operation
            assignment_result = future.result()
            MessagingResultChecker.check(assignment_result.status)
            self.__update_topic_assignment(assignment_result.assignments, topic)
        except Exception as e:
            logger.error(f"handle query topic: {topic} assignment response raise exception. {e}")

    def __update_topic_assignment(self, assignments, topic):
        try:
            new_assignments = Assignments(assignments)
            if not self.__assignments.get(topic):
                self.__assignments.put(topic, new_assignments)
                new_message_queues = list(new_assignments.message_queues())
            else:
                existed = self.__assignments.get(topic)
                if existed == new_assignments:
                    return
                else:
                    # topic route changed
                    removed_message_queues = Assignments.diff_queues(existed, new_assignments)
                    self.__drop_message_queues(removed_message_queues)
                    new_message_queues = Assignments.diff_queues(new_assignments, existed)
            self.__drop_expired_message_queue()
            self.__process_message_queues(new_message_queues)
        except Exception as e:
            logger.error(f"update topic: {topic}, assignment: {assignments} raise exception, {e}")

    def __query_assignment_req(self, topic, queue):
        req = QueryAssignmentRequest()
        req.topic.name = topic
        req.topic.resource_namespace = self.client_configuration.namespace
        req.group.name = self._consumer_group
        req.group.resource_namespace = self.client_configuration.namespace
        req.endpoints.CopyFrom(queue.endpoints.endpoints)
        return req

    # receive #

    def __execute_receive(self, message_queue, process_queue, attempt_id=None):
        if not self.is_running:
            logger.error(f"queue: {message_queue} end receive, because consumer is not running.")
            return
        if process_queue.dropped:
            logger.error(f"queue: {message_queue} end receive, because queue is dropped. ")
            return
        if not attempt_id:
            attempt_id = self.__generate_attempt_id()

        if process_queue.is_cache_full(self.__queue_threshold(self.__max_cache_message_count),
                                       self.__queue_threshold(self.__max_cache_message_size)):
            self.__execute_receive_later(message_queue, process_queue, attempt_id)
        else:
            future = self.__receive_message_executor.submit(
                functools.partial(self.__receive, message_queue=message_queue, process_queue=process_queue, attempt_id=attempt_id))
            future.add_done_callback(functools.partial(self.__handle_received_message, message_queue=message_queue,
                                                       process_queue=process_queue, attempt_id=attempt_id))

    def __receive(self, message_queue, process_queue, attempt_id):
        try:
            max_message_num = process_queue.max_receive_batch_size(self.__queue_threshold(self.__max_cache_message_count), self.__receive_batch_size)
            req = self._receive_req(message_queue.topic, message_queue, max_message_num, True,
                                    long_polling_timeout=self.__long_polling_timeout, attempt_id=attempt_id)
            return self._receive(message_queue, req, self.__long_polling_timeout + self.client_configuration.request_timeout)
        except Exception as e:
            logger.error(f"{self} receive message error, {e}")
            raise e

    def __handle_received_message(self, future, message_queue, process_queue, attempt_id=None):
        if not self.is_running:
            logger.error(f"{self} end receive, because consumer is not running.")
            return
        try:
            messages = future.result()
            if len(messages) > 0:
                self.client_metrics.receive_after(self.consumer_group, messages)
                process_queue.cache_messages(messages)
                self.__consumption.execute_consume(messages, message_queue, self.consumer_group, self.client_metrics)
            self.__execute_receive(message_queue, process_queue)
        except Exception as e:
            logger.error(f"{self} process received message raise exception, {e}")
            self.__execute_receive_later(message_queue, process_queue, attempt_id)

    def __execute_receive_later(self, message_queue, process_queue, attempt_id):
        time.sleep(PushConsumer.RECEIVE_RETRY_DELAY)
        self.__execute_receive(message_queue, process_queue, attempt_id)

    # consume result #

    def __on_consumption_result(self, consume_result, message, message_queue):
        """ callback for Consumption """
        try:
            self.__ack_or_nack(consume_result, message, message_queue)
        except Exception as e:
            raise e

    def __on_fifo_consumption_result(self, consume_result, message, message_queue):
        """ callback for Fifo Consumption """
        try:
            if consume_result == ConsumeResult.FAILURE:
                self.__discard_fifo_message(message, message_queue)
            else:
                self.__ack_or_nack(consume_result, message, message_queue, fifo=True)
        except Exception as e:
            raise e

    def __ack_or_nack(self, consume_result, message, message_queue, fifo=False):
        # in consume thread
        try:
            if consume_result == ConsumeResult.SUCCESS:
                self.ack(message)
            else:
                delivery_attempt = message.delivery_attempt
                if self.__consumption.backoff_policy:
                    invisible_duration = self.__consumption.backoff_policy.get_next_attempt_delay(delivery_attempt)
                else:
                    invisible_duration = 30
                self.change_invisible_duration(message, invisible_duration)
            self.__evict_message(message, message_queue)
        except Exception as e:
            logger.error(f"ack or nack raise exception, {e}")
            if isinstance(e, BadRequestException) and e.code == Code.INVALID_RECEIPT_HANDLE:
                logger.error(f"{self} failed to ack message due to the invalid receipt handle, forgive to retry, topic: {message.topic}, message_id: {message.message_id}")
                return
            if not fifo:
                self.__execute_ack_or_nack_later(consume_result, message, message_queue)
            else:
                time.sleep(PushConsumer.ACK_MESSAGE_FAILURE_BACKOFF_DELAY)
                self.__ack_or_nack(consume_result, message, message_queue, fifo=True)

    def __execute_ack_or_nack_later(self, consume_result, message, message_queue):
        time.sleep(PushConsumer.ACK_MESSAGE_FAILURE_BACKOFF_DELAY)
        self.__ack_or_nack(consume_result, message, message_queue)

    # forward to dead letter queue

    def __discard_fifo_message(self, message, message_queue):
        try:
            self.__forward_message_to_dead_letter_queue(message)
        except Exception as e:
            logger.error(f"discard message raise exception, topic: {message.topic}, message_id: {message.message_id}, {e}")
            time.sleep(1)
            self.__discard_fifo_message(message, message_queue)
        logger.info(f"forward message to dead letter queue successfully, consumerGroup: {self.consumer_group}, topic: {message.topic}, message_id: {message.message_id}")
        self.__evict_message(message, message_queue)

    def __forward_message_to_dead_letter_queue_req(self, message):
        req = ForwardMessageToDeadLetterQueueRequest()
        req.group.name = self._consumer_group
        req.group.resource_namespace = self.client_configuration.namespace
        req.topic.name = message.topic
        req.topic.resource_namespace = self.client_configuration.namespace
        req.receipt_handle = message.receipt_handle
        req.message_id = message.message_id
        req.delivery_attempt = message.delivery_attempt
        req.max_delivery_attempts = self.__consumption.backoff_policy.max_attempts
        return req

    def __forward_message_to_dead_letter_queue(self, message):
        try:
            future = self.rpc_client.forward_message_to_dead_letter_queue_async(
                message.endpoints,
                self.__forward_message_to_dead_letter_queue_req(message),
                metadata=self._sign(),
                timeout=self.client_configuration.request_timeout,
            )
            res = future.result()
            logger.debug(
                f"consumer[{self._consumer_group}] forward message to dead letter queue response, {res.status}"
            )
            MessagingResultChecker.check(res.status)
        except Exception as e:
            raise e

    # process queue #

    def __process_message_queues(self, message_queues):
        for message_queue in message_queues:
            process_queue = ProcessQueue(message_queue)
            self.__process_queues.put(message_queue, process_queue)
        for message_queue, process_queue in self.__process_queues.items():
            self.__execute_receive(message_queue, process_queue)

    def __drop_message_queues(self, dropped_message_queues):
        for dropped_message_queue in dropped_message_queues:
            dropped_process_queue = self.__process_queues.remove(dropped_message_queue)
            if dropped_process_queue:
                dropped_process_queue.drop()

    def __drop_expired_message_queue(self):
        expired_message_queue = [
            mq for mq, pq in self.__process_queues.items()
            if pq.expired(self.__long_polling_timeout, self.client_configuration.request_timeout)
        ]
        if not expired_message_queue:
            self.__drop_message_queues(expired_message_queue)

    def __generate_attempt_id(self): # noqa
        return str(uuid.uuid4())

    def __queue_threshold(self, threshold): # noqa
        queue_size = len(self.__process_queues.keys())
        if queue_size <= 0:
            return 0
        else:
            return max(1, threshold // queue_size)

    def __evict_message(self, message, message_queue):
        process_queue = self.__process_queues.get(message_queue)
        if process_queue:
            process_queue.evict_message(message)

    def __aggregate_process_queues_by_topic(self, attr_name: str):
        topic_values = {}
        for message_queue, process_queue in self.__process_queues.items():
            topic = message_queue.topic
            value = getattr(process_queue, attr_name)
            topic_values[topic] = topic_values.get(topic, 0) + value
        return [
            {
                "value": total_value,
                "attributes": {
                    "topic": topic,
                    "consumer_group": self.consumer_group,
                    "client_id": self.client_id,
                }
            }
            for topic, total_value in topic_values.items()
        ]

    def __process_queues_cached_count(self):
        return self.__aggregate_process_queues_by_topic("cached_messages_count")

    def __process_queues_cached_bytes(self):
        return self.__aggregate_process_queues_by_topic("cached_messages_bytes")

    def __register_process_queues_gauges(self):
        self.client_metrics.create_push_consumer_process_queue_observable_gauge(
            "rocketmq_consumer_cached_messages",
            self.__process_queues_cached_count
        )
        self.client_metrics.create_push_consumer_process_queue_observable_gauge(
            "rocketmq_consumer_cached_bytes",
            self.__process_queues_cached_bytes
        )

    """ property """

    @property
    def fifo(self):
        return self.__fifo

    @property
    def receive_batch_size(self):
        return self.__receive_batch_size

    @property
    def long_polling_timeout(self):
        return self.__long_polling_timeout
