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

#include "RocketMQ.h"
#include <chrono>
#include <cmath>
#include <cstddef>

ROCKETMQ_NAMESPACE_BEGIN

class BackoffPolicy {
public:
  virtual ~BackoffPolicy() = default;

  virtual std::size_t maxAttempts() const = 0;

  virtual std::chrono::milliseconds backoff(std::size_t attempt) const = 0;
};

class ExponentialBackoffPolicy : public BackoffPolicy {
public:
  ExponentialBackoffPolicy(std::size_t max_attempts, std::chrono::milliseconds initial_backoff,
                           std::chrono::milliseconds max_backoff, std::size_t multiplier)
      : max_attempts_(max_attempts), initial_backoff_(initial_backoff), max_backoff_(max_backoff),
        multiplier_(multiplier) {
  }

  std::size_t maxAttempts() const override {
    return max_attempts_;
  }

  std::chrono::milliseconds backoff(std::size_t attempt) const override {
    if (attempt <= 1) {
      return initial_backoff_;
    }

    if (attempt >= max_attempts_) {
      return max_backoff_;
    }

    auto duration = initial_backoff_ * static_cast<std::size_t>(std::pow(multiplier_, attempt));
    if (duration > max_backoff_) {
      return max_backoff_;
    }
    return duration;
  }

private:
  std::size_t max_attempts_;
  std::chrono::milliseconds initial_backoff_;
  std::chrono::milliseconds max_backoff_;
  std::size_t multiplier_;
};

ROCKETMQ_NAMESPACE_END