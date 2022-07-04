/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "RetryPolicy.h"
#include "gtest/gtest.h"

ROCKETMQ_NAMESPACE_BEGIN

TEST(RetryPolicyTest, testBackoff) {
  RetryPolicy policy;
  policy.max_attempt = 3;
  policy.strategy = BackoffStrategy::Customized;
  policy.initial = absl::Milliseconds(0);
  policy.max = absl::Milliseconds(0);
  policy.multiplier = 0.0f;
  policy.next = {absl::Milliseconds(10), absl::Milliseconds(100), absl::Milliseconds(500)};

  ASSERT_EQ(policy.backoff(1), 10);
  ASSERT_EQ(policy.backoff(2), 100);
  ASSERT_EQ(policy.backoff(3), 500);
  ASSERT_EQ(policy.backoff(4), 500);
  ASSERT_EQ(policy.backoff(10000), 500);
}

ROCKETMQ_NAMESPACE_END