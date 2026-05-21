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
#include <random>

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
    seconds_ = deltaSeconds();
    slot.seconds = absl::big_endian::FromHost32(seconds_);
    slot.sequence = absl::big_endian::FromHost32(sequence_);
    sequence_++;
  }
  std::array<uint8_t, 17> raw{};
  raw[0] = VERSION;

  // 9 bytes prefix: VERSION(1) + MAC(6) + PID(low2)
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

std::string UniqueIdGenerator::nextUuidV4Std() {
  std::array<uint8_t, 16> b{};

  static thread_local std::random_device rd;
  for (size_t i = 0; i < b.size();) {
    uint32_t v = static_cast<uint32_t>(rd());
    for (int k = 0; k < 4 && i < b.size(); ++k, ++i) {
      b[i] = static_cast<uint8_t>(v & 0xFF);
      v >>= 8;
    }
  }

  // RFC 4122
  b[6] = static_cast<uint8_t>((b[6] & 0x0F) | 0x40);
  b[8] = static_cast<uint8_t>((b[8] & 0x3F) | 0x80);

  static constexpr char hex[] = "0123456789abcdef";

  std::string out;
  out.resize(36);

  auto put_byte = [&](size_t& p, uint8_t x) {
      out[p++] = hex[(x >> 4) & 0xF];
      out[p++] = hex[x & 0xF];
  };

  size_t p = 0;
  for (int i = 0; i < 16; ++i) {
    if (i == 4 || i == 6 || i == 8 || i == 10) {
      out[p++] = '-';
    }
    put_byte(p, b[i]);
  }

  return out;
}

ROCKETMQ_NAMESPACE_END