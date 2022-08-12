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
#include <gtest/gtest.h>

#include <thread>

#include "AdminServerImpl.h"
#include "apache/rocketmq/v2/admin.grpc.pb.h"
#include "rocketmq/RocketMQ.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/spdlog.h"

namespace rmq = apache::rocketmq::v2;

ROCKETMQ_NAMESPACE_BEGIN
namespace admin {

TEST(AdminServerTest, testSetUp) {
  auto logger = spdlog::basic_logger_mt("rocketmq_logger", "logs/test.log");
  logger->set_level(spdlog::level::debug);
  spdlog::set_default_logger(logger);

  AdminServer* admin_server = new AdminServerImpl;
  admin_server->start();

  std::string address("127.0.0.1:");
  address.append(std::to_string(admin_server->port()));
  auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());

  auto stub = rmq::Admin::NewStub(channel);

  rmq::ChangeLogLevelRequest request;
  request.set_level(rmq::ChangeLogLevelRequest_Level_INFO);
  rmq::ChangeLogLevelResponse response;

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));

  auto status = stub->ChangeLogLevel(&context, request, &response);

  EXPECT_TRUE(status.ok());
  EXPECT_STREQ("OK", response.remark().c_str());
  EXPECT_EQ(spdlog::level::info, logger->level());

  admin_server->stop();

  delete admin_server;
}

}  // namespace admin
ROCKETMQ_NAMESPACE_END