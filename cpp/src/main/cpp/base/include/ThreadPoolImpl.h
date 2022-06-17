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

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "asio.hpp"
#include "asio/io_context.hpp"

#include "ThreadPool.h"
#include "rocketmq/State.h"

ROCKETMQ_NAMESPACE_BEGIN

class ThreadPoolImpl : public ThreadPool {
public:
  explicit ThreadPoolImpl(std::uint16_t workers);

  ~ThreadPoolImpl() override = default;

  void start() override;

  void shutdown() override;

  void submit(std::function<void(void)> task) override;

private:
  asio::io_context context_;
  std::unique_ptr<asio::executor_work_guard<asio::io_context::executor_type>> work_guard_;
  std::uint16_t workers_;
  std::vector<std::thread> threads_;
  std::atomic<State> state_{State::CREATED};
  absl::Mutex start_mtx_;
  absl::CondVar start_cv_;
};

ROCKETMQ_NAMESPACE_END