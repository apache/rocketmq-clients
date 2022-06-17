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
#include "RateLimiter.h"

ROCKETMQ_NAMESPACE_BEGIN

RateLimiterObserver::RateLimiterObserver() : stopped_(false) {
  tick_thread_ = std::thread([this] {
    while (!stopped_.load(std::memory_order_relaxed)) {
      {
        std::lock_guard<std::mutex> lk(members_mtx_);
        for (auto it = members_.begin(); it != members_.end();) {
          std::shared_ptr<Tick> tick = it->lock();
          if (!tick) {
            it = members_.erase(it);
            continue;
          } else {
            ++it;
          }
          tick->tick();
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  });
}

void RateLimiterObserver::subscribe(const std::shared_ptr<Tick>& tick) {
  std::lock_guard<std::mutex> lk(members_mtx_);
  members_.emplace_back(tick);
}

void RateLimiterObserver::stop() {
  stopped_.store(true, std::memory_order_relaxed);
  if (tick_thread_.joinable()) {
    tick_thread_.join();
  }
}

ROCKETMQ_NAMESPACE_END