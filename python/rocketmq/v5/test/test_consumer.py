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
import unittest
from unittest.mock import patch

from rocketmq.v5.client import Client
from rocketmq.v5.client.client_route_manager import ClientRouteManager
from rocketmq.v5.client.connection import RpcClient
from rocketmq.v5.consumer import SimpleConsumer
from rocketmq.v5.model import FilterExpression, Message
from rocketmq.v5.test import TestBase


async def fake_receive_message(*args, **kwargs):
    async def response_stream():
        for response in TestBase.fake_receive_receipt():
            yield response

    return response_stream()


class TestConsumer(unittest.TestCase):

    @patch.object(Message, "_Message__message_body_check_sum")
    @patch.object(RpcClient, "receive_message", side_effect=fake_receive_message)
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
    @patch.object(ClientRouteManager, "update_topic_route", return_value=None)
    def test_receive(
        self,
        mock_update_topic_route,
        mock_start_scheduler,
        mock_select_topic_for_receive,
        mock_select_topic_queue,
        mock_receive_message,
        mock_message_body_check_sum,
    ):
        decode_thread_names = []
        mock_message_body_check_sum.side_effect = (
            lambda *args, **kwargs: decode_thread_names.append(
                threading.current_thread().name
            )
        )
        subs = {TestBase.FAKE_TOPIC_0: FilterExpression()}
        consumer = SimpleConsumer(
            TestBase.fake_client_config(), TestBase.FAKE_CONSUMER_GROUP_0, subs
        )
        consumer.startup()
        try:
            messages = consumer.receive(32, 10)
            async_messages = consumer.receive_async(32, 10).result(timeout=3)

            self.assertIsInstance(messages[0], Message)
            self.assertIsInstance(async_messages[0], Message)
        finally:
            consumer.shutdown()

        mock_update_topic_route.assert_called_once()
        mock_start_scheduler.assert_called_once()
        self.assertEqual(2, mock_select_topic_queue.call_count)
        self.assertEqual(2, mock_select_topic_for_receive.call_count)
        self.assertEqual(2, mock_message_body_check_sum.call_count)
        self.assertEqual(2, mock_receive_message.call_count)
        self.assertTrue(
            all("message_decode_thread" in name for name in decode_thread_names)
        )

        expected_timeout = (
            consumer.client_configuration.request_timeout + consumer.await_duration
        )
        selected_queue = mock_select_topic_queue.return_value
        for receive_call in mock_receive_message.call_args_list:
            args, kwargs = receive_call
            request = args[1]
            self.assertEqual(selected_queue.endpoints, args[0])
            self.assertEqual(selected_queue.message_queue0(), request.message_queue)
            self.assertEqual(32, request.batch_size)
            self.assertEqual(10, request.invisible_duration.seconds)
            self.assertEqual(consumer.await_duration, request.long_polling_timeout.seconds)
            self.assertFalse(request.auto_renew)
            self.assertTrue(kwargs["metadata"])
            self.assertEqual(expected_timeout, kwargs["timeout"])
