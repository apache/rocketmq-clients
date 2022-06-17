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

#include <array>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

class Tick {
public:
  virtual ~Tick() = default;

  virtual void tick() = 0;
};

class RateLimiterObserver {
public:
  RateLimiterObserver();

  void subscribe(const std::shared_ptr<Tick>& tick);

  void stop();

private:
  std::vector<std::weak_ptr<Tick>> members_;
  std::mutex members_mtx_;

  std::atomic_bool stopped_;

  std::thread tick_thread_;
};

template <int PARTITION>
class RateLimiter : public Tick {
public:
  explicit RateLimiter(uint32_t permit) : permits_{0}, interval_(1000 / PARTITION) {
    uint32_t avg = permit / PARTITION;
    for (auto& i : partition_) {
      i = avg;
    }

    uint32_t r = permit % PARTITION;

    if (r) {
      uint32_t step = PARTITION / r;
      for (uint32_t i = 0; i < r; ++i) {
        partition_[i * step]++;
      }
    }
  }

  ~RateLimiter() override = default;

  std::array<uint32_t, PARTITION>& partition() {
    return permits_;
  }

  int slot() {
    auto current = std::chrono::steady_clock::now();
    long ms = std::chrono::duration_cast<std::chrono::milliseconds>(current.time_since_epoch()).count();
    return ms / interval_ % PARTITION;
  }

  uint32_t available() {
    uint32_t available_permit = 0;
    int idx = slot();
    // Reuse quota of the past half of the cycle
    for (int j = 0; j <= PARTITION / 2; ++j) {
      int index = idx - j;
      if (index < 0) {
        index += PARTITION;
      }
      if (permits_[index] > 0) {
        available_permit += permits_[index];
      }
    }

    return available_permit;
  }

  uint32_t acquire(uint32_t permit) {
    int idx = slot();
    {
      std::unique_lock<std::mutex> lk(mtx_);
      if (permits_[idx] >= permit) {
        permits_[idx] -= permit;
        return permit;
      }

      uint32_t acquired = 0;
      if (permits_[idx] > 0) {
        acquired += permits_[idx];
        permits_[idx] = 0;
      }

      // Reuse quota of the past half of the cycle
      for (int j = 1; j <= PARTITION / 2; ++j) {
        int index = idx - j;
        if (index < 0) {
          index += PARTITION;
        }
        while (permits_[index] > 0) {
          --permits_[index];
          if (++acquired >= permit) {
            break;
          }
        }
      }
      return acquired;
    }
  }

  void acquire() {
    int idx = slot();
    {
      std::unique_lock<std::mutex> lk(mtx_);
      if (permits_[idx] > 0) {
        --permits_[idx];
        return;
      }

      // Reuse quota of the past half of the cycle
      for (int j = 1; j <= PARTITION / 2; ++j) {
        int index = idx - j;
        if (index < 0) {
          index += PARTITION;
        }
        if (permits_[index] > 0) {
          --permits_[index];
          return;
        }
      }

      cv_.wait(lk, [this]() {
        int idx = slot();
        return permits_[idx] > 0;
      });
      idx = slot();
      --permits_[idx];
    }
  }

  void tick() override {
    std::this_thread::sleep_for(std::chrono::milliseconds(1000 / PARTITION));
    int idx = slot();
    {
      std::unique_lock<std::mutex> lk(mtx_);
      permits_[idx] = partition_[idx];
      cv_.notify_all();
    }
  }

private:
  std::array<uint32_t, PARTITION> partition_;
  std::array<uint32_t, PARTITION> permits_;
  int interval_;
  std::mutex mtx_;
  std::condition_variable cv_;
};

ROCKETMQ_NAMESPACE_END