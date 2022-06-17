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
#pragma once

#include <cmath>
#include <cstdint>
#include <vector>

#include "absl/time/time.h"
#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

enum class BackoffStrategy : std::uint8_t
{
  Customized = 0,
  Exponential = 1,
};

struct RetryPolicy {
  std::uint32_t max_attempt;

  BackoffStrategy strategy;

  // Exponential back-off
  absl::Duration initial;
  absl::Duration max;
  float multiplier;

  // User defined back-off intervals.
  std::vector<absl::Duration> next;

  std::int64_t backoff(std::size_t attempt) {
    static std::int64_t default_backoff = 1000;
    if (!attempt) {
      attempt = 1;
    }

    switch (strategy) {
      case BackoffStrategy::Customized: {
        if (next.empty()) {
          return default_backoff;
        }
        if (attempt >= next.size()) {
          return absl::ToInt64Milliseconds(next[next.size() - 1]);
        }
        return absl::ToInt64Milliseconds(next[attempt - 1]);
      }

      case BackoffStrategy::Exponential: {
        if (!absl::ToInt64Milliseconds(max)) {
          return default_backoff;
        }

        auto result = initial * pow(2, attempt);
        if (result > max) {
          return absl::ToInt64Milliseconds(max);
        }
        return absl::ToInt64Milliseconds(result);
      }
      default:
        return default_backoff;
    }
  }
};

ROCKETMQ_NAMESPACE_END