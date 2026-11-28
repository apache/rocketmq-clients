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
from types import SimpleNamespace
from unittest.mock import MagicMock

from rocketmq.grpc_protocol import Code, definition_pb2
from rocketmq.v5.consumer.push.message_listener import ConsumeResult
from rocketmq.v5.consumer.push.push_consumer import PushConsumer
from rocketmq.v5.exception import BadRequestException
from rocketmq.v5.model import FilterExpression, Message
from rocketmq.v5.model.retry_policy import CustomizedBackoffRetryPolicy
from rocketmq.v5.test import TestBase


def fake_customized_policy(max_attempts, durations_seconds):
    policy = definition_pb2.RetryPolicy()
    policy.max_attempts = max_attempts
    for seconds in durations_seconds:
        policy.customized_backoff.next.add().seconds = seconds
    return policy


def fake_message(delivery_attempt=None, message_id="fake-id"):
    message = Message()
    message.topic = TestBase.FAKE_TOPIC_0
    message.message_id = message_id
    message._Message__receipt_handle = TestBase.FAKE_RECEIPT_HANDLE_0
    if delivery_attempt is not None:
        message._Message__delivery_attempt = delivery_attempt
    return message


class TestPushConsumerNack(unittest.TestCase):

    def setUp(self):
        self._original_backoff_delay = PushConsumer.ACK_MESSAGE_FAILURE_BACKOFF_DELAY
        # Speed up the retry loop under test.
        PushConsumer.ACK_MESSAGE_FAILURE_BACKOFF_DELAY = 0
        self.consumer = PushConsumer(
            TestBase.fake_client_config(),
            TestBase.FAKE_CONSUMER_GROUP_0,
            lambda message: None,
            {TestBase.FAKE_TOPIC_0: FilterExpression()},
        )
        self.consumer._Client__is_running = True
        self.consumer._PushConsumer__consumption = SimpleNamespace(
            backoff_policy=CustomizedBackoffRetryPolicy(
                None, PushConsumer.DEFAULT_CONSUME_ATTEMPTS)
        )

    def tearDown(self):
        PushConsumer.ACK_MESSAGE_FAILURE_BACKOFF_DELAY = self._original_backoff_delay

    def run_ack_or_nack(self, consume_result, message, process_queue=None, fifo=False):
        finished = threading.Event()
        thread = threading.Thread(
            target=lambda: (
                self.consumer._PushConsumer__ack_or_nack(
                    consume_result, message, None, process_queue, fifo=fifo),
                finished.set(),
            ),
            daemon=True,
        )
        thread.start()
        self.assertTrue(finished.wait(timeout=10),
                        "ack_or_nack did not finish, the retry loop is unbounded")
        thread.join(timeout=1)

    def test_nack_with_unset_delivery_attempt_treated_as_first_attempt(self):
        # A message whose delivery_attempt was omitted by the server (proto3
        # scalar -> None) must be nacked once with the default delay instead of
        # crashing the retry loop with a TypeError.
        self.consumer._change_invisible_duration = MagicMock()
        self.run_ack_or_nack(ConsumeResult.FAILURE, fake_message(delivery_attempt=None))
        self.consumer._change_invisible_duration.assert_called_once()
        self.assertEqual(self.consumer._change_invisible_duration.call_args[0][1], 1)

    def test_nack_uses_delivery_attempt_from_message(self):
        self.consumer._PushConsumer__consumption = SimpleNamespace(
            backoff_policy=CustomizedBackoffRetryPolicy(
                fake_customized_policy(17, [2, 4, 6]), 17))
        self.consumer._change_invisible_duration = MagicMock()
        self.run_ack_or_nack(ConsumeResult.FAILURE, fake_message(delivery_attempt=3))
        self.consumer._change_invisible_duration.assert_called_once()
        self.assertEqual(self.consumer._change_invisible_duration.call_args[0][1], 6)

    def test_nack_failure_retries_are_capped_and_message_evicted(self):
        self.consumer._change_invisible_duration = MagicMock(
            side_effect=Exception("rpc unavailable"))
        process_queue = MagicMock()
        self.run_ack_or_nack(ConsumeResult.FAILURE, fake_message(delivery_attempt=1),
                             process_queue=process_queue)
        self.assertEqual(self.consumer._change_invisible_duration.call_count,
                         PushConsumer.MAX_ACK_OR_NACK_ATTEMPTS)
        process_queue.evict_message.assert_called_once()

    def test_fifo_nack_failure_retries_are_capped_and_message_evicted(self):
        self.consumer._change_invisible_duration = MagicMock(
            side_effect=Exception("rpc unavailable"))
        process_queue = MagicMock()
        self.run_ack_or_nack(ConsumeResult.FAILURE, fake_message(delivery_attempt=1),
                             process_queue=process_queue, fifo=True)
        self.assertEqual(self.consumer._change_invisible_duration.call_count,
                         PushConsumer.MAX_ACK_OR_NACK_ATTEMPTS)
        process_queue.evict_message.assert_called_once()

    def test_ack_success_evicts_message_once(self):
        self.consumer._ack = MagicMock()
        process_queue = MagicMock()
        self.run_ack_or_nack(ConsumeResult.SUCCESS, fake_message(delivery_attempt=1),
                             process_queue=process_queue)
        self.consumer._ack.assert_called_once()
        process_queue.evict_message.assert_called_once()

    def test_invalid_receipt_handle_gives_up_without_retry(self):
        self.consumer._ack = MagicMock(
            side_effect=BadRequestException("invalid handle", Code.INVALID_RECEIPT_HANDLE))
        self.consumer._change_invisible_duration = MagicMock()
        process_queue = MagicMock()
        self.run_ack_or_nack(ConsumeResult.SUCCESS, fake_message(delivery_attempt=1),
                             process_queue=process_queue)
        self.consumer._change_invisible_duration.assert_not_called()
        process_queue.evict_message.assert_called_once()

    def test_dlq_request_defaults_unset_delivery_attempt_to_zero(self):
        # The DLQ forward request copies delivery_attempt into an int32 proto
        # field; an unset value must be written as 0 (unset), not raise.
        request = self.consumer._PushConsumer__forward_message_to_dead_letter_queue_req(
            fake_message(delivery_attempt=None))
        self.assertEqual(request.delivery_attempt, 0)
        request = self.consumer._PushConsumer__forward_message_to_dead_letter_queue_req(
            fake_message(delivery_attempt=5))
        self.assertEqual(request.delivery_attempt, 5)


if __name__ == "__main__":
    unittest.main()
