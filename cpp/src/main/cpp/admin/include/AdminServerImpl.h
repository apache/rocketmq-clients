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
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

#include "AdminServiceImpl.h"
#include "rocketmq/AdminServer.h"

#include "grpcpp/grpcpp.h"

using grpc::Server;

ROCKETMQ_NAMESPACE_BEGIN

namespace admin {
class AdminServerImpl : public AdminServer {
public:
  AdminServerImpl()
      : port_(0), state_(State::CREATED), async_stub_(new rmq::Admin::AsyncService), service_(new AdminServiceImpl) {
  }

  bool start() override;

  bool stop() override;

  int port() const override {
    return port_;
  }

private:
  int port_;
  std::unique_ptr<Server> server_;
  std::atomic<State> state_;
  std::unique_ptr<rmq::Admin::AsyncService> async_stub_;
  std::unique_ptr<rmq::Admin::Service> service_;
  std::unique_ptr<grpc::ServerCompletionQueue> completion_queue_;
  std::thread loop_thread_;

  std::mutex loop_mtx_;
  std::condition_variable loop_cv_;
  void loop();
};
} // namespace admin

ROCKETMQ_NAMESPACE_END