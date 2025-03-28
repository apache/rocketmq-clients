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
from rocketmq.v5.consumer import SimpleConsumer
from rocketmq.v5.model import FilterExpression, Message
from rocketmq.v5.test import TestBase


class TestNormalConsumer(unittest.TestCase):

    @patch.object(Message, "_Message__message_body_check_sum")
    @patch.object(SimpleConsumer, "_SimpleConsumer__receive_message_response")
    @patch.object(RpcClient, "receive_message_async")
    @patch.object(
        SimpleConsumer,
        "_SimpleConsumer__select_topic_queue",
        return_value=TestBase.fake_queue(TestBase.FAKE_TOPIC_0),
    )
    @patch.object(
        SimpleConsumer,
        "_SimpleConsumer__select_topic_for_receive",
        return_value=TestBase.FAKE_TOPIC_0,
    )
    @patch.object(Client, "_Client__start_scheduler", return_value=None)
    @patch.object(Client, "_Client__update_topic_route", return_value=None)
    def test_receive(
        self,
        mock_update_topic_route,
        mock_start_scheduler,
        mock_select_topic_for_receive,
        mock_select_topic_queue,
        mock_receive_message_async,
        mock_receive_message_response,
        mock_message_body_check_sum,
    ):
        future = Future()
        future.set_result(list())
        mock_receive_message_async.return_value = future
        mock_receive_message_response.return_value = TestBase.fake_receive_receipt()

        subs = {TestBase.FAKE_TOPIC_0: FilterExpression()}
        consumer = SimpleConsumer(
            TestBase.fake_client_config(), TestBase.FAKE_CONSUMER_GROUP_0, subs
        )
        consumer.startup()
        messages = consumer.receive(32, 10)
        self.assertIsInstance(messages[0], Message)
        consumer.shutdown()

        mock_update_topic_route.assert_called()
        mock_start_scheduler.assert_called_once()
        mock_select_topic_queue.assert_called_once()
        mock_select_topic_for_receive.assert_called_once()
        mock_message_body_check_sum.assert_called_once()
        mock_receive_message_response.assert_called_once()
        mock_receive_message_async.assert_called_once()
