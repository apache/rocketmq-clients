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
from unittest.mock import MagicMock

from google.protobuf.duration_pb2 import Duration
from rocketmq.grpc_protocol import definition_pb2
from rocketmq.v5.consumer.push.push_consumer import (PushConsumer,
                                                     _create_backoff_policy)
from rocketmq.v5.model import Message
from rocketmq.v5.model.retry_policy import (CustomizedBackoffRetryPolicy,
                                            ExponentialBackoffRetryPolicy)
from rocketmq.v5.test import TestBase
from rocketmq.v5.util import Misc


def exponential_settings(max_attempts):
    settings = definition_pb2.Settings()
    settings.backoff_policy.max_attempts = max_attempts
    settings.backoff_policy.exponential_backoff.initial.seconds = 1
    settings.backoff_policy.exponential_backoff.max.seconds = 60
    settings.backoff_policy.exponential_backoff.multiplier = 2.0
    return settings


def customized_settings(max_attempts):
    settings = definition_pb2.Settings()
    settings.backoff_policy.max_attempts = max_attempts
    settings.backoff_policy.customized_backoff.next.add().seconds = 2
    settings.backoff_policy.customized_backoff.next.add().seconds = 5
    return settings


class TestBackoffPolicyDispatch(unittest.TestCase):
    """The push consumer must honor both strategies issued by the proxy.

    Mirrors Java PushSubscriptionSettings, which switches over
    RetryPolicy.strategyCase and supports exponential as well as customized
    backoff; previously only customized was honored.
    """

    def test_exponential_strategy_dispatched(self):
        policy = _create_backoff_policy(exponential_settings(5), PushConsumer.DEFAULT_CONSUME_ATTEMPTS)
        self.assertIsInstance(policy, ExponentialBackoffRetryPolicy)
        self.assertEqual(policy.max_attempts, 5)
        self.assertEqual(policy.get_next_attempt_delay(2), 2)

    def test_customized_strategy_dispatched(self):
        policy = _create_backoff_policy(customized_settings(6), PushConsumer.DEFAULT_CONSUME_ATTEMPTS)
        self.assertIsInstance(policy, CustomizedBackoffRetryPolicy)
        self.assertEqual(policy.max_attempts, 6)
        self.assertEqual(policy.durations, [2, 5])

    def test_missing_strategy_falls_back_to_default(self):
        policy = _create_backoff_policy(definition_pb2.Settings(), PushConsumer.DEFAULT_CONSUME_ATTEMPTS)
        self.assertIsInstance(policy, CustomizedBackoffRetryPolicy)
        self.assertEqual(policy.max_attempts, PushConsumer.DEFAULT_CONSUME_ATTEMPTS)
        self.assertEqual(policy.durations, [])

    def test_none_settings_falls_back_to_default(self):
        policy = _create_backoff_policy(None, PushConsumer.DEFAULT_CONSUME_ATTEMPTS)
        self.assertIsInstance(policy, CustomizedBackoffRetryPolicy)
        self.assertEqual(policy.max_attempts, PushConsumer.DEFAULT_CONSUME_ATTEMPTS)

    def test_settings_update_replaces_policy_across_types(self):
        # A consumer configured with a customized policy must accept an
        # exponential policy from a later settings update (cross-type
        # inequality must not raise).
        old_policy = _create_backoff_policy(customized_settings(6), 17)
        new_policy = _create_backoff_policy(exponential_settings(5), 17)
        self.assertNotEqual(old_policy, new_policy)
        self.assertNotEqual(new_policy, old_policy)


class TestProtoDurationConversion(unittest.TestCase):

    def test_integer_seconds(self):
        duration = Duration()
        Misc.fill_proto_duration(duration, 3)
        self.assertEqual(duration.seconds, 3)
        self.assertEqual(duration.nanos, 0)

    def test_float_seconds_keep_sub_second_precision(self):
        # Fractional seconds previously raised
        # "TypeError: 'float' object cannot be interpreted as an integer"
        # when assigned directly to the int64 seconds field.
        duration = Duration()
        Misc.fill_proto_duration(duration, 2.5)
        self.assertEqual(duration.seconds, 2)
        self.assertEqual(duration.nanos, 500000000)

        duration = Duration()
        Misc.fill_proto_duration(duration, 0.5)
        self.assertEqual(duration.seconds, 0)
        self.assertEqual(duration.nanos, 500000000)

    def test_none_leaves_duration_untouched(self):
        duration = Duration()
        Misc.fill_proto_duration(duration, None)
        self.assertEqual(duration.seconds, 0)
        self.assertEqual(duration.nanos, 0)

    def test_receive_request_accepts_float_invisible_duration(self):
        from rocketmq.v5.consumer.push.push_consumer import PushConsumer as PC
        consumer = PC(
            TestBase.fake_client_config(),
            TestBase.FAKE_CONSUMER_GROUP_0,
            lambda message: None,
            {TestBase.FAKE_TOPIC_0: None},
        )
        request = consumer._receive_req(
            "topic-not-subscribed", TestBase.fake_queue(TestBase.FAKE_TOPIC_0),
            10, False, invisible_duration=2.5)
        self.assertEqual(request.invisible_duration.seconds, 2)
        self.assertEqual(request.invisible_duration.nanos, 500000000)

    def test_change_invisible_request_accepts_float_seconds(self):
        from rocketmq.v5.consumer.push.push_consumer import PushConsumer as PC
        consumer = PC(
            TestBase.fake_client_config(),
            TestBase.FAKE_CONSUMER_GROUP_0,
            lambda message: None,
            {TestBase.FAKE_TOPIC_0: None},
        )
        message = Message()
        message.topic = TestBase.FAKE_TOPIC_0
        message.message_id = "fake-id"
        message._Message__receipt_handle = TestBase.FAKE_RECEIPT_HANDLE_0
        request = consumer._Consumer__change_invisible_req(message, 1.5)
        self.assertEqual(request.invisible_duration.seconds, 1)
        self.assertEqual(request.invisible_duration.nanos, 500000000)
        request = consumer._Consumer__change_invisible_req(message, 3)
        self.assertEqual(request.invisible_duration.seconds, 3)
        self.assertEqual(request.invisible_duration.nanos, 0)


if __name__ == "__main__":
    unittest.main()
