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
#include "grpcpp/grpcpp.h"
#include "rocketmq/RocketMQ.h"

using grpc::ServerContext;
using grpc::Status;

namespace rmq = apache::rocketmq::v2;

ROCKETMQ_NAMESPACE_BEGIN

namespace admin {
class AdminServiceImpl final : public rmq::Admin::Service {
public:
  Status ChangeLogLevel(ServerContext* context, const rmq::ChangeLogLevelRequest* request,
                        rmq::ChangeLogLevelResponse* reply) override;
};
} // namespace admin

ROCKETMQ_NAMESPACE_END