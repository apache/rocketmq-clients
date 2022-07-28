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
#include "CountdownLatch.h"

#include "rocketmq/Logger.h"
#include "spdlog/spdlog.h"

ROCKETMQ_NAMESPACE_BEGIN

void CountdownLatch::await() {
  absl::MutexLock lock(&mtx_);
  if (count_ <= 0) {
    return;
  }
  while (count_ > 0) {
    cv_.Wait(&mtx_);
  }
}

void CountdownLatch::countdown() {
  absl::MutexLock lock(&mtx_);
  if (--count_ <= 0) {
    cv_.SignalAll();
  }
  if (!name_.empty()) {
    if (count_ >= 0) {
      SPDLOG_TRACE("After countdown(), latch[{}]={}", name_, count_);
    }
  }
}

void CountdownLatch::increaseCount() {
  absl::MutexLock lock(&mtx_);
  ++count_;
  if (!name_.empty()) {
    SPDLOG_TRACE("After increaseCount(), latch[{}]={}", name_, count_);
  }
}

ROCKETMQ_NAMESPACE_END