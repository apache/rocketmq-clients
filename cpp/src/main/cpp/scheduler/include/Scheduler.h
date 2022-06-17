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
#include <functional>
#include <memory>
#include <string>

#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

class Scheduler {
public:
  virtual ~Scheduler() = default;

  virtual void start() = 0;

  virtual void shutdown() = 0;

  virtual std::uint32_t schedule(const std::function<void(void)>& functor, const std::string& task_name,
                                 std::chrono::milliseconds delay, std::chrono::milliseconds interval) = 0;

  virtual void cancel(std::uint32_t task_id) = 0;
};

using SchedulerPtr = std::weak_ptr<Scheduler>;
using SchedulerSharedPtr = std::shared_ptr<Scheduler>;

ROCKETMQ_NAMESPACE_END