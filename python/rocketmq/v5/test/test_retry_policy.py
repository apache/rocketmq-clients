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

from rocketmq.grpc_protocol import definition_pb2
from rocketmq.v5.exception import IllegalArgumentException
from rocketmq.v5.model.retry_policy import (CustomizedBackoffRetryPolicy,
                                            ExponentialBackoffRetryPolicy)


def fake_customized_policy(max_attempts, durations_seconds):
    policy = definition_pb2.RetryPolicy()
    policy.max_attempts = max_attempts
    for seconds in durations_seconds:
        policy.customized_backoff.next.add().seconds = seconds
    return policy


def fake_exponential_policy(max_attempts, initial_seconds, max_seconds, multiplier):
    policy = definition_pb2.RetryPolicy()
    policy.max_attempts = max_attempts
    policy.exponential_backoff.initial.seconds = initial_seconds
    policy.exponential_backoff.max.seconds = max_seconds
    policy.exponential_backoff.multiplier = multiplier
    return policy


class TestCustomizedBackoffRetryPolicy(unittest.TestCase):

    def test_get_next_attempt_delay_values(self):
        policy = CustomizedBackoffRetryPolicy(fake_customized_policy(3, [1, 2, 3]), 17)
        self.assertEqual(policy.get_next_attempt_delay(1), 1)
        self.assertEqual(policy.get_next_attempt_delay(2), 2)
        self.assertEqual(policy.get_next_attempt_delay(3), 3)
        # Beyond the configured list, the last duration is reused.
        self.assertEqual(policy.get_next_attempt_delay(4), 3)
        self.assertEqual(policy.get_next_attempt_delay(100), 3)

    def test_get_next_attempt_delay_rejects_non_positive_attempt(self):
        # attempt is 1-based; 0 and negative attempts must be rejected instead
        # of silently indexing the last duration via Python's negative indexing.
        policy = CustomizedBackoffRetryPolicy(fake_customized_policy(3, [1, 2, 3]), 17)
        self.assertRaises(IllegalArgumentException, policy.get_next_attempt_delay, 0)
        self.assertRaises(IllegalArgumentException, policy.get_next_attempt_delay, -1)

    def test_default_without_server_policy(self):
        policy = CustomizedBackoffRetryPolicy(None, 17)
        self.assertEqual(policy.max_attempts, 17)
        self.assertEqual(policy.get_next_attempt_delay(1), 1)
        self.assertEqual(policy.get_next_attempt_delay(5), 1)

    def test_eq_between_same_type(self):
        left = CustomizedBackoffRetryPolicy(fake_customized_policy(3, [1, 2]), 17)
        right = CustomizedBackoffRetryPolicy(fake_customized_policy(3, [1, 2]), 17)
        other = CustomizedBackoffRetryPolicy(fake_customized_policy(3, [2, 1]), 17)
        self.assertEqual(left, right)
        self.assertNotEqual(left, other)


class TestExponentialBackoffRetryPolicy(unittest.TestCase):

    def test_get_next_attempt_delay_values_and_cap(self):
        policy = ExponentialBackoffRetryPolicy(fake_exponential_policy(5, 1, 60, 2.0), 17)
        self.assertEqual(policy.get_next_attempt_delay(1), 1)
        self.assertEqual(policy.get_next_attempt_delay(2), 2)
        self.assertEqual(policy.get_next_attempt_delay(3), 4)
        self.assertEqual(policy.get_next_attempt_delay(7), 60)  # capped by max

    def test_get_next_attempt_delay_rejects_non_positive_attempt(self):
        policy = ExponentialBackoffRetryPolicy(fake_exponential_policy(5, 1, 60, 2.0), 17)
        self.assertRaises(IllegalArgumentException, policy.get_next_attempt_delay, 0)
        self.assertRaises(IllegalArgumentException, policy.get_next_attempt_delay, -1)

    def test_zero_initial_backoff_is_immediate(self):
        # A zero initial backoff is a valid "retry immediately" configuration
        # (cf. Java ExponentialBackoffRetryPolicy#immediatelyRetryPolicy); it
        # must not be treated as "unset".
        policy = ExponentialBackoffRetryPolicy(fake_exponential_policy(3, 0, 60, 2.0), 17)
        self.assertEqual(policy.get_next_attempt_delay(1), 0)
        self.assertEqual(policy.get_next_attempt_delay(2), 0)

    def test_default_without_server_policy(self):
        policy = ExponentialBackoffRetryPolicy(None, 3)
        self.assertEqual(policy.max_attempts, 3)
        self.assertEqual(policy.get_next_attempt_delay(1), 1)

    def test_delay_is_seconds_float(self):
        policy = ExponentialBackoffRetryPolicy(fake_exponential_policy(5, 1, 60, 1.5), 17)
        delay = policy.get_next_attempt_delay(2)
        self.assertIsInstance(delay, float)
        self.assertEqual(delay, 1.5)

    def test_eq_with_other_policy_type_does_not_raise(self):
        # Cross-type comparison must return NotImplemented instead of raising
        # AttributeError on missing private attributes.
        exponential = ExponentialBackoffRetryPolicy(fake_exponential_policy(3, 1, 60, 2.0), 17)
        customized = CustomizedBackoffRetryPolicy(fake_customized_policy(3, [1]), 17)
        self.assertFalse(exponential == customized)
        self.assertTrue(exponential != customized)


if __name__ == "__main__":
    unittest.main()
