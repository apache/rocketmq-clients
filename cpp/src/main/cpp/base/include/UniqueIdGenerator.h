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

#include <chrono>
#include <cstdint>
#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"

#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

class UniqueIdGenerator {
public:
  static UniqueIdGenerator& instance();

  std::string next() LOCKS_EXCLUDED(mtx_);

  UniqueIdGenerator(const UniqueIdGenerator&) = delete;

  UniqueIdGenerator(UniqueIdGenerator&&) = delete;

  UniqueIdGenerator& operator=(const UniqueIdGenerator&) = delete;

  UniqueIdGenerator& operator=(UniqueIdGenerator&&) = delete;

private:
  UniqueIdGenerator();

  struct Slot {
    uint32_t seconds;
    uint32_t sequence;
  };

  static std::chrono::system_clock::time_point customEpoch();

  /**
   * Seconds since 2021-01-01 00:00:00.0 +0000
   * @return
   */
  uint32_t deltaSeconds();

  std::array<uint8_t, 8> prefix_;
  absl::Mutex mtx_;

  /**
   * Duration since 2021-01-01 00:00:00.0(UTC)
   */
  std::chrono::system_clock::duration since_custom_epoch_;

  /**
   * @brief Generator created time point using steady clock. Such that when system time are adjusted backward using NTP
   * service, generated message identifiers remain unique.
   *
   * Note: when program restarts, duplicated message-id may still get generated. However, if program run in
   * containerized environment, say Kubernetes, this will NOT be a problem since MAC address would change on restart.
   */
  std::chrono::steady_clock::time_point start_time_point_;

  uint32_t seconds_;
  uint32_t sequence_;
  static const uint8_t VERSION;
};

ROCKETMQ_NAMESPACE_END