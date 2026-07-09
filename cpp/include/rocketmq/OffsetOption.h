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

#include <cstdint>
#include <stdexcept>

#include "RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

/**
 * @brief Specifies where to start consuming messages when subscribing to a lite topic.
 *
 * OffsetOption provides multiple strategies for controlling the initial consumption offset:
 * - Policy-based: LAST (resume from last position), MIN (earliest available), MAX (latest available)
 * - Absolute offset: start from a specific queue offset
 * - Tail N: consume the last N messages
 * - Timestamp: consume messages from a specific point in time
 */
class OffsetOption {
public:
  enum class Type : std::uint8_t {
    POLICY = 0,
    OFFSET = 1,
    TAIL_N = 2,
    TIMESTAMP = 3,
  };

  enum class Policy : std::int64_t {
    LAST = 0,
    MIN = 1,
    MAX = 2,
  };

  /**
   * Resume from the last consumed position.
   */
  static OffsetOption lastOffset() {
    return OffsetOption(Type::POLICY, static_cast<std::int64_t>(Policy::LAST));
  }

  /**
   * Start from the earliest available message.
   */
  static OffsetOption minOffset() {
    return OffsetOption(Type::POLICY, static_cast<std::int64_t>(Policy::MIN));
  }

  /**
   * Start from the latest available message.
   */
  static OffsetOption maxOffset() {
    return OffsetOption(Type::POLICY, static_cast<std::int64_t>(Policy::MAX));
  }

  /**
   * Start from a specific queue offset.
   *
   * @param offset Absolute offset position (must be >= 0).
   */
  static OffsetOption ofOffset(std::int64_t offset) {
    if (offset < 0) {
      throw std::invalid_argument("offset must be >= 0");
    }
    return OffsetOption(Type::OFFSET, offset);
  }

  /**
   * Consume the last N messages.
   *
   * @param tail_n Number of trailing messages (must be >= 0).
   */
  static OffsetOption ofTailN(std::int64_t tail_n) {
    if (tail_n < 0) {
      throw std::invalid_argument("tail_n must be >= 0");
    }
    return OffsetOption(Type::TAIL_N, tail_n);
  }

  /**
   * Consume messages from a specific timestamp.
   *
   * @param timestamp Unix timestamp in milliseconds (must be >= 0).
   */
  static OffsetOption ofTimestamp(std::int64_t timestamp) {
    if (timestamp < 0) {
      throw std::invalid_argument("timestamp must be >= 0");
    }
    return OffsetOption(Type::TIMESTAMP, timestamp);
  }

  Type type() const { return type_; }
  std::int64_t value() const { return value_; }

  bool operator==(const OffsetOption& other) const {
    return type_ == other.type_ && value_ == other.value_;
  }

  bool operator!=(const OffsetOption& other) const {
    return !(*this == other);
  }

private:
  OffsetOption(Type type, std::int64_t value) : type_(type), value_(value) {}

  Type type_;
  std::int64_t value_;
};

ROCKETMQ_NAMESPACE_END
