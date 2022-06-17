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
#include "AdminServerImpl.h"

#include "AdminServiceImpl.h"
#include "ServerCall.h"
#include "spdlog/spdlog.h"
#include <memory>

using grpc::ServerBuilder;

ROCKETMQ_NAMESPACE_BEGIN

namespace admin {
bool AdminServerImpl::start() {
  if (State::CREATED != state_) {
    return false;
  }
  State current = State::CREATED;
  if (state_.compare_exchange_weak(current, State::STARTING, std::memory_order_relaxed)) {
    std::string server_address("127.0.0.1:");
    server_address.append(std::to_string(port_));

    grpc::EnableDefaultHealthCheckService(true);

    ServerBuilder server_builder;
    server_builder.AddListeningPort(server_address, grpc::InsecureServerCredentials(), &port_);
    server_builder.RegisterService(async_stub_.get());
    // A gRPC server may have multiple per-thread CompletionQueue
    completion_queue_ = server_builder.AddCompletionQueue();
    server_ = server_builder.BuildAndStart();
    {
      loop();
      std::unique_lock<std::mutex> lk(loop_mtx_);
      loop_cv_.wait(lk, [this]() { return State::STARTED == state_.load(); });
    }
    SPDLOG_INFO("Admin server started and listening 127.0.0.1:{}", port_);
    return true;
  }
  return false;
}

void AdminServerImpl::loop() {
  auto loop_lambda = [this] {
    state_.store(State::STARTED);
    {
      std::unique_lock<std::mutex> lk(loop_mtx_);
      loop_cv_.notify_all();
    }

    SPDLOG_DEBUG("Prepare initial ServerCall");
    new ServerCall(async_stub_.get(), service_.get(), completion_queue_.get());
    void* tag;
    bool ok;

    while (completion_queue_->Next(&tag, &ok)) {
      if (!ok) {
        delete static_cast<ServerCall*>(tag);
        break;
      }
      static_cast<ServerCall*>(tag)->proceed();
    }
  };
  loop_thread_ = std::thread(loop_lambda);
}

bool AdminServerImpl::stop() {
  if (State::STARTED != state_.load()) {
    return false;
  }

  if (server_) {
    State expected = State::STARTED;
    if (state_.compare_exchange_strong(expected, State::STOPPING, std::memory_order_relaxed)) {
      SPDLOG_INFO("Stopping admin server");
      server_->Shutdown();

      if (completion_queue_) {
        completion_queue_->Shutdown();
        // Drain completion_queue_
        {
          void* tag;
          bool ignore_ok;
          while (completion_queue_->Next(&tag, &ignore_ok)) {
          }
        }
      }

      if (loop_thread_.joinable()) {
        loop_thread_.join();
      }

      state_.store(State::STOPPED);
      SPDLOG_INFO("Admin server stopped");
      return true;
    }
    return false;
  } else {
    SPDLOG_ERROR("Admin server is unexpected nullptr");
  }
  return false;
}
} // namespace admin

ROCKETMQ_NAMESPACE_END