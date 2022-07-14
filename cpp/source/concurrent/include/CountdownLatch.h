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

#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"

#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

class CountdownLatch {
public:
  explicit CountdownLatch(int32_t count) : CountdownLatch(count, "anonymous") {
  }
  CountdownLatch(int32_t count, absl::string_view name) : count_(count), name_(name.data(), name.length()) {
  }

  void await() LOCKS_EXCLUDED(mtx_);

  void countdown() LOCKS_EXCLUDED(mtx_);

  void increaseCount() LOCKS_EXCLUDED(mtx_);

private:
  int32_t count_ GUARDED_BY(mtx_);

  absl::Mutex mtx_; // protects count_
  absl::CondVar cv_;

  std::string name_;
};

ROCKETMQ_NAMESPACE_END