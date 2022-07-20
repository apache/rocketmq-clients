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
#include "ThreadPoolImpl.h"
#include "absl/memory/memory.h"
#include "asio/executor_work_guard.hpp"
#include "asio/io_context.hpp"
#include "asio/post.hpp"
#include "rocketmq/RocketMQ.h"
#include "rocketmq/State.h"
#include "spdlog/spdlog.h"
#include <atomic>
#include <cstdint>
#include <exception>
#include <system_error>

ROCKETMQ_NAMESPACE_BEGIN

ThreadPoolImpl::ThreadPoolImpl(std::uint16_t workers)
    : work_guard_(
          absl::make_unique<asio::executor_work_guard<asio::io_context::executor_type>>(context_.get_executor())),
      workers_(workers) {
}

void ThreadPoolImpl::start() {
  for (std::uint16_t i = 0; i < workers_; i++) {
    std::thread worker([this]() {
      State expected = State::CREATED;
      if (state_.compare_exchange_strong(expected, State::STARTED, std::memory_order_relaxed)) {
        absl::MutexLock lk(&start_mtx_);
        start_cv_.SignalAll();
      }

      while (true) {
#ifdef __EXCEPTIONS
        try {
#endif
          std::error_code ec;
          context_.run(ec);
          if (ec) {
            SPDLOG_WARN("Error raised from ThreadPool: {}", ec.message());
          }
#ifdef __EXCEPTIONS
        } catch (std::exception& e) {
          SPDLOG_WARN("Exception raised from ThreadPool: {}", e.what());
        }
#endif
        if (State::STARTED != state_.load(std::memory_order_relaxed)) {
          SPDLOG_DEBUG("One thread-pool worker quit");
          break;
        }
      }
    });
    threads_.emplace_back(std::move(worker));
  }

  {
    absl::MutexLock lk(&start_mtx_);
    if (State::CREATED == state_.load(std::memory_order_relaxed)) {
      start_cv_.Wait(&start_mtx_);
    }
  }
}

void ThreadPoolImpl::shutdown() {
  State expected = State::STARTED;
  if (state_.compare_exchange_strong(expected, State::STOPPING, std::memory_order_relaxed)) {
    work_guard_->reset();
    context_.stop();
    for (auto& thread : threads_) {
      if (thread.joinable()) {
        thread.join();
      }
    }
    state_.store(State::STOPPED, std::memory_order_relaxed);
  }
}

void ThreadPoolImpl::submit(std::function<void()> task) {
  if (State::STARTED == state_.load(std::memory_order_relaxed)) {
    asio::post(context_, task);
  } else {
    SPDLOG_WARN("State of ThreadPool is not STARTED");
  }
}

ROCKETMQ_NAMESPACE_END