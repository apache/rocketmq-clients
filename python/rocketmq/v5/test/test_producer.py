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
from unittest.mock import patch

from rocketmq.v5.client import Client
from rocketmq.v5.client.connection import RpcClient
from rocketmq.v5.model import SendReceipt
from rocketmq.v5.producer import Producer
from rocketmq.v5.test import TestBase


class TestNormalProducer(unittest.TestCase):

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
