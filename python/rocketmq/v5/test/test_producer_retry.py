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

import time
import unittest
from concurrent.futures import Future
from unittest.mock import patch

from rocketmq.v5.client import Client
from rocketmq.v5.client.connection import RpcClient
from rocketmq.v5.exception import TooManyRequestsException
from rocketmq.v5.model import Message
from rocketmq.v5.producer import Producer
from rocketmq.v5.test import TestBase


class ControlledRetryPolicy(object):
    """Deterministic backoff policy with measurable, short delays."""

    def __init__(self, max_attempts=3, delay=0.0):
        self.max_attempts = max_attempts
        self.delay = delay
        self.delay_requests = []

    def get_next_attempt_delay(self, attempt):
        self.delay_requests.append(attempt)
        return self.delay


def failing_rpc(exc):
    """Return a side effect that fails every send with the given exception."""
    calls = {"count": 0}

    def side_effect(*args, **kwargs):
        calls["count"] += 1
        future = Future()
        future.set_exception(exc)
        return future

    return side_effect, calls


class TestProducerSendRetry(unittest.TestCase):
    """Retry behavior must match the Java ProducerImpl: throttled requests
    (TooManyRequestsException) are retried after a backoff delay, any other
    failure is retried immediately."""

    def build_producer(self, retry_policy, side_effect):
        producer = Producer(TestBase.fake_client_config())
        producer._Producer__backoff_policy = retry_policy
        return producer, side_effect

    @patch.object(
        Producer,
        "_Producer__select_send_queue",
        return_value=TestBase.fake_queue(TestBase.FAKE_TOPIC_0),
    )
    @patch.object(Client, "_Client__start_scheduler", return_value=None)
    def test_sync_send_too_many_requests_retried_with_backoff(
        self, mock_start_scheduler, mock_select_send_queue
    ):
        side_effect, calls = failing_rpc(
            TooManyRequestsException("too many requests", "TOO_MANY_REQUESTS"))
        policy = ControlledRetryPolicy(max_attempts=3, delay=0.05)
        with patch.object(RpcClient, "send_message_async", side_effect=side_effect):
            producer, _ = self.build_producer(policy, side_effect)
            producer.startup()
            message = TestBase.fake_send_message(TestBase.FAKE_TOPIC_0)
            start = time.perf_counter()
            with self.assertRaises(TooManyRequestsException):
                producer.send(message)
            elapsed = time.perf_counter() - start
            producer.shutdown()
        self.assertEqual(calls["count"], 3)
        # Two retries, each waiting for the backoff delay.
        self.assertEqual(policy.delay_requests, [2, 3])
        self.assertGreaterEqual(elapsed, 0.09)

    @patch.object(
        Producer,
        "_Producer__select_send_queue",
        return_value=TestBase.fake_queue(TestBase.FAKE_TOPIC_0),
    )
    @patch.object(Client, "_Client__start_scheduler", return_value=None)
    def test_sync_send_generic_failure_retried_immediately(
        self, mock_start_scheduler, mock_select_send_queue
    ):
        side_effect, calls = failing_rpc(Exception("boom"))
        policy = ControlledRetryPolicy(max_attempts=3, delay=0.05)
        with patch.object(RpcClient, "send_message_async", side_effect=side_effect):
            producer, _ = self.build_producer(policy, side_effect)
            producer.startup()
            message = TestBase.fake_send_message(TestBase.FAKE_TOPIC_0)
            start = time.perf_counter()
            with self.assertRaises(Exception):
                producer.send(message)
            elapsed = time.perf_counter() - start
            producer.shutdown()
        self.assertEqual(calls["count"], 3)
        # No backoff delay is requested or awaited for non-throttled failures.
        self.assertEqual(policy.delay_requests, [])
        self.assertLess(elapsed, 0.05)

    @patch.object(
        Producer,
        "_Producer__select_send_queue",
        return_value=TestBase.fake_queue(TestBase.FAKE_TOPIC_0),
    )
    @patch.object(Client, "_Client__start_scheduler", return_value=None)
    def test_async_send_too_many_requests_retried_with_backoff(
        self, mock_start_scheduler, mock_select_send_queue
    ):
        side_effect, calls = failing_rpc(
            TooManyRequestsException("too many requests", "TOO_MANY_REQUESTS"))
        policy = ControlledRetryPolicy(max_attempts=3, delay=0.05)
        with patch.object(RpcClient, "send_message_async", side_effect=side_effect):
            producer, _ = self.build_producer(policy, side_effect)
            producer.startup()
            message = TestBase.fake_send_message(TestBase.FAKE_TOPIC_0)
            start = time.perf_counter()
            future = producer.send_async(message)
            with self.assertRaises(TooManyRequestsException):
                future.result(timeout=10)
            elapsed = time.perf_counter() - start
            producer.shutdown()
        self.assertEqual(calls["count"], 3)
        self.assertEqual(policy.delay_requests, [2, 3])
        self.assertGreaterEqual(elapsed, 0.09)


if __name__ == "__main__":
    unittest.main()
