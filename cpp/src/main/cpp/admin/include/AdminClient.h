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
#include "apache/rocketmq/v2/admin.grpc.pb.h"
#include "rocketmq/RocketMQ.h"
#include <grpcpp/grpcpp.h>
#include <memory>

namespace rmq = apache::rocketmq::v2;

ROCKETMQ_NAMESPACE_BEGIN

using grpc::Channel;
using grpc::Status;

namespace admin {

class AdminClient {
public:
  explicit AdminClient(std::shared_ptr<Channel>& channel) : stub_(rmq::Admin::NewStub(channel)) {
  }

  Status changeLogLevel(const rmq::ChangeLogLevelRequest& request, rmq::ChangeLogLevelResponse& response);

private:
  std::unique_ptr<rmq::Admin::Stub> stub_;
};

} // namespace admin

ROCKETMQ_NAMESPACE_END