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
#include "UniqueIdGenerator.h"

#include <cstring>

#include "spdlog/spdlog.h"
#include "MixAll.h"
#include "UtilAll.h"
#include "absl/base/internal/endian.h"


#ifdef _WIN32
#include <process.h>
#endif

ROCKETMQ_NAMESPACE_BEGIN

const uint8_t UniqueIdGenerator::VERSION = 1;

UniqueIdGenerator::UniqueIdGenerator()
    : prefix_(), since_custom_epoch_(std::chrono::system_clock::now() - customEpoch()),
      start_time_point_(std::chrono::steady_clock::now()), seconds_(deltaSeconds()), sequence_(0) {
  std::vector<unsigned char> mac_address;
  if (UtilAll::macAddress(mac_address)) {
    memcpy(prefix_.data(), mac_address.data(), mac_address.size());
  } else {
    SPDLOG_WARN("Failed to get network interface MAC address");
  }

#ifdef _WIN32
  int pid = _getpid();
#else
  pid_t pid = getpid();
#endif

  uint32_t big_endian_pid = absl::big_endian::FromHost32(pid);
  // Copy the lower 2 bytes
  memcpy(prefix_.data() + 6, reinterpret_cast<uint8_t*>(&big_endian_pid) + 2, sizeof(uint16_t));
}

UniqueIdGenerator& UniqueIdGenerator::instance() {
  static UniqueIdGenerator generator;
  return generator;
}

std::string UniqueIdGenerator::next() {
  Slot slot = {};
  {
    absl::MutexLock lk(&mtx_);
    uint32_t delta = deltaSeconds();
    if (seconds_ != delta) {
      seconds_ = delta;
      sequence_ = 0;
      SPDLOG_DEBUG("Second: {} and sequence: {}", seconds_, sequence_);
    } else {
      sequence_++;
    }
    slot.seconds = seconds_;
    slot.sequence = sequence_;
  }
  std::array<uint8_t, 17> raw{};
  raw[0] = VERSION;
  memcpy(raw.data() + sizeof(VERSION), prefix_.data(), prefix_.size());
  memcpy(raw.data() + sizeof(VERSION) + prefix_.size(), &slot, sizeof(slot));
  return MixAll::hex(raw.data(), raw.size());
}

std::chrono::system_clock::time_point UniqueIdGenerator::customEpoch() {
  return absl::ToChronoTime(absl::FromDateTime(2021, 1, 1, 0, 0, 0, absl::UTCTimeZone()));
}

uint32_t UniqueIdGenerator::deltaSeconds() {
  return std::chrono::duration_cast<std::chrono::seconds>((std::chrono::steady_clock::now() - start_time_point_) +
                                                          since_custom_epoch_)
      .count();
}

ROCKETMQ_NAMESPACE_END