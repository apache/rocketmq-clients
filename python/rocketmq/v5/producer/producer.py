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

import abc
import functools
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor

from rocketmq.grpc_protocol import (ClientType, Code, Encoding,
                                    EndTransactionRequest, HeartbeatRequest,
                                    MessageType,
                                    NotifyClientTerminationRequest, Publishing,
                                    RecallMessageRequest, SendMessageRequest,
                                    Settings, TelemetryCommand,
                                    TransactionResolution, TransactionSource)
from rocketmq.v5.client import Client
from rocketmq.v5.client.balancer import QueueSelector
from rocketmq.v5.exception import (ClientException, IllegalArgumentException,
                                   IllegalStateException,
                                   TooManyRequestsException)
from rocketmq.v5.log import logger
from rocketmq.v5.model import CallbackResult, Message, SendReceipt
from rocketmq.v5.util import (ConcurrentMap, MessageIdCodec,
                              MessagingResultChecker, Misc)


class Transaction:
    __transaction_lock = threading.Lock()

    def __init__(self, producer):
        self.__message = None
        self.__send_receipt = None
        self.__producer = producer

    def add_half_message(self, message: Message):
        with Transaction.__transaction_lock:
            if message is None:
                raise IllegalArgumentException(
                    "add half message error, message is none."
                )

            if self.__message is None:
                self.__message = message
            else:
                raise IllegalArgumentException(
                    f"message already existed in transaction, topic:{message.topic}"
                )

    def add_send_receipt(self, send_receipt):
        with Transaction.__transaction_lock:
            if self.__message is None:
                raise IllegalArgumentException(
                    "add send receipt error, no message in transaction."
                )
            if send_receipt is None:
                raise IllegalArgumentException(
                    "add send receipt error, send receipt in none."
                )
            if self.__message.message_id != send_receipt.message_id:
                raise IllegalArgumentException(
                    "can't add another send receipt to a half message."
                )

            self.__send_receipt = send_receipt

    def commit(self):
        return self.__commit_or_rollback(TransactionResolution.COMMIT)

    def rollback(self):
        return self.__commit_or_rollback(TransactionResolution.ROLLBACK)

    def __commit_or_rollback(self, result):
        if self.__message is None:
            raise IllegalArgumentException("no message in transaction.")
        if self.__send_receipt is None or self.__send_receipt.transaction_id is None:
            raise IllegalArgumentException(
                "no transaction_id in transaction, must send half message at first."
            )

        try:
            res = self.__producer.end_transaction(
                self.__send_receipt.message_queue.endpoints,
                self.__message,
                self.__send_receipt.transaction_id,
                result,
                TransactionSource.SOURCE_CLIENT,
            )
            if res.status.code != Code.OK:
                logger.error(
                    f"transaction commit or rollback error. topic:{self.__message.topic}, message_id:{self.__message.message_id}, transaction_id:{self.__send_receipt.transaction_id}, transactionResolution:{result}"
                )
                raise ClientException(res.status.message, res.status.code)
            return res
        except Exception as e:
            logger.error(
                f"end transaction error, topic:{self.__message.topic}, message_id:{self.__send_receipt.message_id}, transaction_id:{self.__send_receipt.transaction_id}, transactionResolution:{result}: {e}"
            )
            raise e

    """ property """

    @property
    def message_id(self):
        return self.__message.message_id


class TransactionChecker(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def check(self, message: Message) -> TransactionResolution:
        pass


class Producer(Client):
    MAX_SEND_ATTEMPTS = 3  # max retry times when send failed

    def __init__(
        self, client_configuration, topics=None, checker=None, tls_enable=False
    ):
        super().__init__(client_configuration, topics, ClientType.PRODUCER, tls_enable)
        # {topic, QueueSelector}
        self.__send_queue_selectors = ConcurrentMap()
        self.__checker = (
            checker  # checker for transaction message, handle checking from server
        )
        self.__transaction_check_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="transaction_check_thread")

    def __str__(self):
        return f"{ClientType.Name(self.client_type)} client_id:{self.client_id}"

    # send message #

    def send(self, message: Message, transaction=None) -> SendReceipt:
        if not self.is_running:
            raise IllegalStateException("producer is not running now.")

        self.__wrap_sending_message(message, False if transaction is None else True)
        topic_queue = self.__select_send_queue(message)
        if message.message_type not in topic_queue.accept_message_types:
            raise IllegalArgumentException(
                f"current message type not match with queue accept message types, topic:{message.topic}, message_type:{Message.message_type_desc(message.message_type)}, queue access type:{topic_queue.accept_message_types_desc()}"
            )

        if transaction is None:
            try:
                return self.__send(message, topic_queue)
            except Exception as e:
                logger.error(f"send message exception, topic: {message.topic}, e: {e}")
                raise e
        else:
            try:
                transaction.add_half_message(message)
                send_receipt = self.__send(message, topic_queue)
                message.message_id = send_receipt.message_id
                transaction.add_send_receipt(send_receipt)
                return send_receipt
            except IllegalArgumentException as e:
                raise e
            except Exception as e:
                logger.error(
                    f"send transaction message exception, topic: {message.topic}, e: {e}"
                )
                raise e

    def send_async(self, message: Message):
        if not self.is_running:
            raise IllegalStateException("producer is not running now.")

        self.__wrap_sending_message(message, False)
        topic_queue = self.__select_send_queue(message)
        if message.message_type not in topic_queue.accept_message_types:
            raise IllegalArgumentException(
                f"current message type not match with queue accept message types, "
                f"topic:{message.topic}, message_type:{Message.message_type_desc(message.message_type)}, "
                f"queue access type:{topic_queue.accept_message_types_desc()}"
            )

        try:
            return self.__send_async(message, topic_queue)
        except Exception as e:
            logger.error(f"send message exception, topic: {message.topic}, {e}")
            raise e

    # recall timer #

    def recall_message(self, topic, recall_handle: str):
        try:
            future = self.__recall_message(topic, recall_handle)
            return self.__handle_recall_result(future)
        except Exception as e:
            raise e

    def recall_message_async(self, topic, recall_handle: str):
        try:
            future = self.__recall_message(topic, recall_handle)
            ret_future = Future()
            recall_message_callback = functools.partial(
                self.__handle_recall_result, ret_future=ret_future
            )
            future.add_done_callback(recall_message_callback)
            return ret_future
        except Exception as e:
            raise e

    # transaction #

    def begin_transaction(self):
        if not self.is_running:
            raise IllegalStateException(
                "unable to begin transaction because producer is not running"
            )

        if self.__checker is None:
            raise IllegalArgumentException("Transaction checker should not be null.")
        return Transaction(self)

    def end_transaction(self, endpoints, message, transaction_id, result, source):
        if not self.is_running:
            raise IllegalStateException(
                "unable to end transaction because producer is not running"
            )

        if self.__checker is None:
            raise IllegalArgumentException("Transaction checker should not be null.")

        req = self.__end_transaction_req(message, transaction_id, result, source)
        future = self.rpc_client.end_transaction_async(
            endpoints,
            req,
            metadata=self._sign(),
            timeout=self.client_configuration.request_timeout,
        )
        return future.result()

    def on_recover_orphaned_transaction_command(
        self, endpoints, msg, transaction_id
    ):
        # call this function from server side stream, in RpcClient._io_loop
        try:
            if not self.is_running:
                raise IllegalStateException(
                    "unable to recover orphaned transaction command because producer is not running"
                )

            if self.__checker is None:
                raise IllegalArgumentException("No transaction checker registered.")
            message = Message().fromProtobuf(msg)
            self.__transaction_check_executor.submit(self.__server_transaction_check, endpoints, message, transaction_id)
        except Exception as e:
            logger.error(f"on_recover_orphaned_transaction_command exception: {e}")

    """ override """

    def _on_start(self):
        logger.info(f"{self} start success.")

    def _on_start_failure(self):
        logger.error(f"{self} start failed.")

    def _sync_setting_req(self, endpoints):
        # publishing
        pub = Publishing()
        topics = self.topics
        for topic in topics:
            resource = pub.topics.add()
            resource.name = topic
            resource.resource_namespace = self.client_configuration.namespace
        pub.max_body_size = 1024 * 1024 * 128
        pub.validate_message_type = True

        # setting
        settings = Settings()
        settings.client_type = self.client_type
        settings.access_point.CopyFrom(endpoints.endpoints)
        settings.request_timeout.seconds = self.client_configuration.request_timeout
        settings.publishing.CopyFrom(pub)

        settings.user_agent.language = Misc.sdk_language()
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
        return req

    def _notify_client_termination_req(self):
        return NotifyClientTerminationRequest()

    def _update_queue_selector(self, topic, topic_route):
        queue_selector = self.__send_queue_selectors.get(topic)
        if queue_selector is None:
            return
        queue_selector.update(topic_route)

    def shutdown(self):
        logger.info(f"begin to shutdown {self}")
        self.__transaction_check_executor.shutdown()
        self.__transaction_check_executor = None
        super().shutdown()
        logger.info(f"shutdown {self} success.")

    """ private """

    # send #

    def __send(self, message: Message, topic_queue, attempt=1) -> SendReceipt:
        req = self.__send_req(message)
        send_context = self.client_metrics.send_before(message.topic)
        send_message_future = self.rpc_client.send_message_async(
            topic_queue.endpoints,
            req,
            self._sign(),
            timeout=self.client_configuration.request_timeout,
        )
        return self.__handle_send_response_sync(
            send_message_future, message, topic_queue, attempt, send_context
        )

    def __handle_send_response_sync(
        self,
        send_message_future,
        message,
        topic_queue,
        attempt,
        send_metric_context=None,
    ):
        try:
            send_receipt = self.__handle_send_result(
                send_message_future, topic_queue
            )
            self.client_metrics.send_after(send_metric_context, True)
            return send_receipt
        except Exception as e:
            attempt += 1
            retry_exception_future = self.__check_send_retry_condition(
                message, topic_queue, attempt, e
            )
            if retry_exception_future is not None:
                # end retry with exception
                self.client_metrics.send_after(send_metric_context, False)
                raise retry_exception_future.exception()

            # resend message
            topic_queue = self.__select_send_queue(message)
            return self.__send(message, topic_queue, attempt)

    def __send_async(self, message: Message, topic_queue, attempt=1, ret_future=None):
        req = self.__send_req(message)
        send_context = self.client_metrics.send_before(message.topic)
        send_message_future = self.rpc_client.send_message_async(
            topic_queue.endpoints,
            req,
            self._sign(),
            timeout=self.client_configuration.request_timeout,
        )
        if ret_future is None:
            ret_future = Future()
        handle_send_receipt_callback = functools.partial(
            self.__handle_send_response_async,
            message=message,
            topic_queue=topic_queue,
            attempt=attempt,
            ret_future=ret_future,
            send_metric_context=send_context,
        )
        send_message_future.add_done_callback(handle_send_receipt_callback)
        return ret_future

    def __handle_send_response_async(
        self,
        send_message_future,
        message,
        topic_queue,
        attempt,
        ret_future,
        send_metric_context=None,
    ):
        try:
            send_receipt = self.__handle_send_result(
                send_message_future, topic_queue
            )
            self.client_metrics.send_after(send_metric_context, True)
            self._submit_callback(
                CallbackResult.async_send_callback_result(ret_future, send_receipt)
            )
        except Exception as e:
            attempt += 1
            retry_exception_future = self.__check_send_retry_condition(
                message, topic_queue, attempt, e
            )
            if retry_exception_future is not None:
                # end retry with exception
                self.client_metrics.send_after(send_metric_context, False)
                self._submit_callback(
                    CallbackResult.async_send_callback_result(
                        ret_future, retry_exception_future.exception(), False
                    )
                )
                return
            # resend message
            topic_queue = self.__select_send_queue(message)
            self.__send_async(message, topic_queue, attempt, ret_future)

    def __handle_send_result(self, send_message_future, topic_queue):
        res = send_message_future.result()
        MessagingResultChecker.check(res.status)
        entries = res.entries
        assert (
            len(entries) == 1
        ), f"entries size error, the send response entries size is {len(entries)}, {self}"
        entry = entries[0]
        return SendReceipt(
            entry.message_id, entry.transaction_id, topic_queue, entry.offset, entry.recall_handle if entry.recall_handle else None
        )

    def __check_send_retry_condition(self, message, topic_queue, attempt, e):
        end_retry = False
        if attempt > Producer.MAX_SEND_ATTEMPTS:
            logger.error(
                f"{self} failed to send message to {topic_queue.endpoints}, because of run out of attempt times, topic:{message.topic}, message_id:{message.message_id},  message_type:{message.message_type}, attempt:{attempt}"
            )
            end_retry = True

        # end retry if system busy
        if isinstance(e, TooManyRequestsException):
            logger.error(
                f"{self} failed to send message to {topic_queue.endpoints}, because of to too many requests, topic:{message.topic},  message_type:{message.message_type}, message_id:{message.message_id}, attempt:{attempt}"
            )
            end_retry = True

        if end_retry:
            send_exception_future = Future()
            send_exception_future.set_exception(e)
            return send_exception_future
        else:
            return None

    def __wrap_sending_message(self, message, is_transaction):
        message.message_id = MessageIdCodec().next_message_id()
        message.message_type = self.__send_message_type(message, is_transaction)

    def __send_req(self, message: Message):
        try:
            req = SendMessageRequest()
            msg = req.messages.add()
            msg.topic.name = message.topic
            msg.topic.resource_namespace = self.client_configuration.namespace
            if message.body is None or len(message.body) == 0:
                raise IllegalArgumentException("message body is none.")
            max_body_size = 4 * 1024 * 1024  # max body size is 4m
            if len(message.body) > max_body_size:
                raise IllegalArgumentException(
                    f"Message body size exceeds the threshold, max size={max_body_size} bytes"
                )

            msg.body = message.body
            if message.tag is not None:
                msg.system_properties.tag = message.tag
            if message.keys is not None:
                msg.system_properties.keys.extend(message.keys)
            if message.properties is not None:
                msg.user_properties.update(message.properties)
            msg.system_properties.message_id = message.message_id
            msg.system_properties.message_type = message.message_type
            msg.system_properties.born_timestamp.seconds = int(time.time())
            msg.system_properties.born_host = Misc.get_local_ip()
            msg.system_properties.body_encoding = Encoding.IDENTITY
            if message.message_group is not None:
                msg.system_properties.message_group = message.message_group
            if message.delivery_timestamp is not None:
                msg.system_properties.delivery_timestamp.seconds = (
                    message.delivery_timestamp
                )
            return req
        except Exception as e:
            raise e

    def __send_message_type(self, message: Message, is_transaction=False):
        if (
            message.message_group is None
            and message.delivery_timestamp is None
            and is_transaction is False
        ):
            return MessageType.NORMAL

        if message.message_group is not None and is_transaction is False:
            return MessageType.FIFO

        if message.delivery_timestamp is not None and is_transaction is False:
            return MessageType.DELAY

        if (
            message.message_group is None
            and message.delivery_timestamp is None
            and is_transaction is True
        ):
            return MessageType.TRANSACTION

        # transaction semantics is conflicted with fifo/delay.
        logger.error(
            f"{self} set send message type exception, message: {str(message)}"
        )
        raise IllegalArgumentException(
            "transactional message should not set messageGroup or deliveryTimestamp"
        )

    def __select_send_queue(self, message):
        try:
            route = self._retrieve_topic_route_data(message.topic)
            queue_selector = self.__send_queue_selectors.put_if_absent(
                message.topic, QueueSelector.producer_queue_selector(route)
            )
            if message.message_group is None:
                return queue_selector.select_next_queue()
            else:
                return queue_selector.select_queue_by_hash_key(message.message_group)
        except Exception as e:
            logger.error(f"producer select topic:{message.topic} queue raise exception, {e}")
            raise e

    # recall timer #

    def __recall_message(self, topic, recall_handle: str):
        if not self.is_running:
            raise IllegalStateException(
                "unable to recall message because producer is not running"
            )
        try:
            return self.rpc_client.recall_message_async(
                self.client_configuration.rpc_endpoints,
                self.__recall_message_req(topic, recall_handle),
                metadata=self._sign(),
                timeout=self.client_configuration.request_timeout,
            )
        except Exception as e:
            raise e

    def __recall_message_req(self, topic, recall_handle: str):
        req = RecallMessageRequest()
        req.topic.name = topic
        req.topic.resource_namespace = self.client_configuration.namespace
        req.recall_handle = recall_handle
        return req

    def __handle_recall_result(self, future, ret_future=None):
        try:
            res = future.result()
            MessagingResultChecker.check(res.status)
            if ret_future is not None:
                self._submit_callback(
                    CallbackResult.recall_message_callback_result(ret_future, res.message_id)
                )
            else:
                return res.message_id
        except Exception as e:
            if ret_future is None:
                raise e
            else:
                self._submit_callback(
                    CallbackResult.recall_message_callback_result(ret_future, e, False)
                )

    # transaction #

    def __end_transaction_req(self, message: Message, transaction_id, result, source):
        req = EndTransactionRequest()
        req.topic.name = message.topic
        req.topic.resource_namespace = self.client_configuration.namespace
        req.message_id = message.message_id
        req.transaction_id = transaction_id
        req.resolution = result
        req.source = source
        return req

    def __server_transaction_check_callback(self, future, message, transaction_id, result):
        try:
            res = future.result()
            if res is not None and res.status.code == Code.OK:
                if result == TransactionResolution.COMMIT:
                    logger.debug(
                        f"{self} commit message. message_id: {message.message_id}, transaction_id: {transaction_id}, res: {res}"
                    )
                elif result == TransactionResolution.ROLLBACK:
                    logger.debug(
                        f"{self} rollback message. message_id: {message.message_id}, transaction_id: {transaction_id}, res: {res}"
                    )
            else:
                if result == TransactionResolution.COMMIT:
                    raise Exception(f"{self} commit message: {message.message_id} raise exception")
                elif result == TransactionResolution.ROLLBACK:
                    raise Exception(f"{self} rollback message: {message.message_id} raise exception")
        except Exception as e:
            logger.error(f"server transaction check raise exception, {e}")

    def __server_transaction_check(self, endpoints, message, transaction_id):
        try:
            result = self.__checker.check(message)
            req = self.__end_transaction_req(message, transaction_id, result, TransactionSource.SOURCE_SERVER_CHECK)
            future = self.rpc_client.end_transaction_async(endpoints, req, metadata=self._sign(), timeout=self.client_configuration.request_timeout)
            future.add_done_callback(functools.partial(self.__server_transaction_check_callback, message=message, transaction_id=transaction_id, result=result))
        except Exception as e:
            raise e
