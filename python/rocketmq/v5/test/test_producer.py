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

import unittest
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

from rocketmq.grpc_protocol import Code, MessageType
from rocketmq.v5.client import Client
from rocketmq.v5.client.connection import RpcClient
from rocketmq.v5.model import SendReceipt
from rocketmq.v5.producer import Producer, TransactionChecker
from rocketmq.v5.test import TestBase
from rocketmq.v5.util import Misc


class TestProducer(unittest.TestCase):

    @patch.object(
        Producer,
        "_Producer__select_send_queue",
        return_value=TestBase.fake_queue(TestBase.FAKE_TOPIC_0),
    )
    @patch.object(RpcClient, "send_message_async")
    @patch.object(Client, "_Client__start_scheduler", return_value=None)
    def test_send(
        self, mock_start_scheduler, mock_send_message_async, mock_select_send_queue
    ):
        # mock send_message_async return future
        future = Future()
        future.set_result(TestBase.fake_send_success_response())
        mock_send_message_async.return_value = future
        producer = Producer(TestBase.fake_client_config())
        producer.startup()
        message = TestBase.fake_send_message(TestBase.FAKE_TOPIC_0)
        result = producer.send(message)
        self.assertIsInstance(result, SendReceipt)
        producer.shutdown()
        mock_start_scheduler.assert_called_once()
        mock_select_send_queue.assert_called_once()
        mock_send_message_async.assert_called_once()

    @patch.object(
        Producer,
        "_Producer__select_send_queue",
        return_value=TestBase.fake_queue(TestBase.FAKE_TOPIC_0),
    )
    @patch.object(RpcClient, "send_message_async")
    @patch.object(Client, "_Client__start_scheduler", return_value=None)
    def test_send_delay_message(
        self, mock_start_scheduler, mock_send_message_async, mock_select_send_queue
    ):
        future = Future()
        future.set_result(TestBase.fake_send_success_response())
        mock_send_message_async.return_value = future

        producer = Producer(TestBase.fake_client_config())
        producer.startup()

        message = TestBase.fake_send_message(TestBase.FAKE_TOPIC_0)
        message.delivery_timestamp = Misc.current_mills() + 60_000  # delay 60 seconds
        result = producer.send(message)
        self.assertIsInstance(result, SendReceipt)
        self.assertEqual(message.message_type, MessageType.DELAY)

        producer.shutdown()
        mock_start_scheduler.assert_called_once()
        mock_select_send_queue.assert_called_once()
        mock_send_message_async.assert_called_once()

    @patch.object(
        Producer,
        "_Producer__select_send_queue",
        return_value=TestBase.fake_queue(TestBase.FAKE_TOPIC_0),
    )
    @patch.object(RpcClient, "send_message_async")
    @patch.object(Client, "_Client__start_scheduler", return_value=None)
    def test_send_fifo_message(
        self, mock_start_scheduler, mock_send_message_async, mock_select_send_queue
    ):
        future = Future()
        future.set_result(TestBase.fake_send_success_response())
        mock_send_message_async.return_value = future

        producer = Producer(TestBase.fake_client_config())
        producer.startup()

        message = TestBase.fake_send_message(TestBase.FAKE_TOPIC_0)
        message.message_group = "fifo-group-0"
        result = producer.send(message)
        self.assertIsInstance(result, SendReceipt)
        self.assertEqual(message.message_type, MessageType.FIFO)

        producer.shutdown()
        mock_start_scheduler.assert_called_once()
        mock_select_send_queue.assert_called_once()
        mock_send_message_async.assert_called_once()

    @patch.object(
        Producer,
        "_Producer__select_send_queue",
        return_value=TestBase.fake_queue(TestBase.FAKE_TOPIC_0),
    )
    @patch.object(RpcClient, "send_message_async")
    @patch.object(Client, "_Client__start_scheduler", return_value=None)
    def test_send_priority_message(
        self, mock_start_scheduler, mock_send_message_async, mock_select_send_queue
    ):
        future = Future()
        future.set_result(TestBase.fake_send_success_response())
        mock_send_message_async.return_value = future

        producer = Producer(TestBase.fake_client_config())
        producer.startup()

        message = TestBase.fake_send_message(TestBase.FAKE_TOPIC_0)
        message.priority = 1
        result = producer.send(message)
        self.assertIsInstance(result, SendReceipt)
        self.assertEqual(message.message_type, MessageType.PRIORITY)

        producer.shutdown()
        mock_start_scheduler.assert_called_once()
        mock_select_send_queue.assert_called_once()
        mock_send_message_async.assert_called_once()

    @patch.object(
        Producer,
        "_Producer__select_send_queue",
        return_value=TestBase.fake_queue(TestBase.FAKE_TOPIC_0),
    )
    @patch.object(RpcClient, "send_message_async")
    @patch.object(Client, "_Client__start_scheduler", return_value=None)
    def test_send_async(
        self, mock_start_scheduler, mock_send_message_async, mock_select_send_queue
    ):
        future = Future()
        future.set_result(TestBase.fake_send_success_response())
        mock_send_message_async.return_value = future

        producer = Producer(TestBase.fake_client_config())
        producer.startup()

        message = TestBase.fake_send_message(TestBase.FAKE_TOPIC_0)
        result_future = producer.send_async(message)
        result = result_future.result()
        self.assertIsInstance(result, SendReceipt)

        producer.shutdown()
        mock_start_scheduler.assert_called_once()
        mock_select_send_queue.assert_called_once()
        mock_send_message_async.assert_called_once()

    @patch.object(RpcClient, "end_transaction_async")
    @patch.object(
        Producer,
        "_Producer__select_send_queue",
        return_value=TestBase.fake_queue(TestBase.FAKE_TOPIC_0),
    )
    @patch.object(RpcClient, "send_message_async")
    @patch.object(Client, "_Client__start_scheduler", return_value=None)
    def test_transaction_commit(
        self,
        mock_start_scheduler,
        mock_send_message_async,
        mock_select_send_queue,
        mock_end_transaction_async,
    ):
        # mock half message send
        send_future = Future()
        send_future.set_result(TestBase.fake_send_success_response())
        mock_send_message_async.return_value = send_future

        # mock commit response
        commit_future = Future()
        commit_future.set_result(TestBase.fake_end_transaction_success_response())
        mock_end_transaction_async.return_value = commit_future

        checker = MagicMock(spec=TransactionChecker)
        producer = Producer(TestBase.fake_client_config(), checker=checker)
        producer.startup()

        # step 1: begin transaction
        tx = producer.begin_transaction()

        # step 2: send half message
        message = TestBase.fake_send_message(TestBase.FAKE_TOPIC_0)
        receipt = producer.send(message, transaction=tx)
        self.assertIsInstance(receipt, SendReceipt)
        self.assertEqual(message.message_type, MessageType.TRANSACTION)

        # step 3: commit
        commit_resp = tx.commit()
        self.assertEqual(commit_resp.status.code, Code.OK)

        producer.shutdown()
        mock_start_scheduler.assert_called_once()
        mock_select_send_queue.assert_called_once()
        mock_send_message_async.assert_called_once()
        mock_end_transaction_async.assert_called_once()

    @patch.object(RpcClient, "recall_message_async")
    @patch.object(Client, "_Client__start_scheduler", return_value=None)
    def test_recall_message(
        self, mock_start_scheduler, mock_recall_message_async
    ):
        recall_future = Future()
        recall_future.set_result(
            TestBase.fake_recall_message_success_response("test-recalled-msg-id")
        )
        mock_recall_message_async.return_value = recall_future

        producer = Producer(TestBase.fake_client_config())
        producer.startup()

        message_id = producer.recall_message(
            TestBase.FAKE_TOPIC_0, "fake-recall-handle"
        )
        self.assertEqual(message_id, "test-recalled-msg-id")

        producer.shutdown()
        mock_start_scheduler.assert_called_once()
        mock_recall_message_async.assert_called_once()
