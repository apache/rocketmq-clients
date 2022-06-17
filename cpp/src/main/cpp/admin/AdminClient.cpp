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
#include "AdminClient.h"
#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

namespace admin {

Status AdminClient::changeLogLevel(const rmq::ChangeLogLevelRequest& request, rmq::ChangeLogLevelResponse& response) {
  grpc::ClientContext context;
  auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(3);
  context.set_deadline(deadline);
  Status status = stub_->ChangeLogLevel(&context, request, &response);
  return status;
}

} // namespace admin

ROCKETMQ_NAMESPACE_END

void wrapChangeLogLevelRequest(rmq::ChangeLogLevelRequest& request, const std::string& level) {
  if (level.empty()) {
    return;
  }

  if ("trace" == level) {
    request.set_level(rmq::ChangeLogLevelRequest_Level_TRACE);
    return;
  }

  if ("debug" == level) {
    request.set_level(rmq::ChangeLogLevelRequest_Level_DEBUG);
    return;
  }

  if ("info" == level) {
    request.set_level(rmq::ChangeLogLevelRequest_Level_INFO);
    return;
  }

  if ("warn" == level) {
    request.set_level(rmq::ChangeLogLevelRequest_Level_WARN);
    return;
  }

  if ("error" == level) {
    request.set_level(rmq::ChangeLogLevelRequest_Level_ERROR);
    return;
  }
}

int main(int argc, char* argv[]) {
  if (argc <= 1) {
    std::cerr << "Usage: 'admin_client level [port]' where level is among trace/debug/info/warn/error.\n";
    return EXIT_SUCCESS;
  }

  int port = 9877;

  char* env_port = getenv("ROCKETMQ_ADMIN_PORT");
  if (nullptr != env_port) {
    try {
      port = std::stoi(env_port);
    } catch (...) {
      std::cerr << "Failed to parse port from environment variable ROCKETMQ_ADMIN_PORT" << env_port << "\n";
    }
  }

  if (argc > 2) {
    try {
      port = std::stoi(argv[2]);
    } catch (...) {
      std::cerr << "Failed to parse port from command line:" << argv[2] << "\n";
    }
  }

  std::string target("127.0.0.1:");
  target.append(std::to_string(port));

  std::cout << "Target address: " << target << "\n";
  auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
  rocketmq::admin::AdminClient client(channel);

  rmq::ChangeLogLevelRequest request;
  wrapChangeLogLevelRequest(request, argv[1]);
  rmq::ChangeLogLevelResponse response;
  auto status = client.changeLogLevel(request, response);
  if (status.ok()) {
    std::cout << "Log level changed OK" << std::endl;
  } else {
    std::cerr << "Failed to change log level. GRPC error code: " << status.error_code()
              << ", GRPC error message: " << status.error_message() << "\n";
    std::cerr << "Server remark: " << response.remark() << std::endl;
  }
  return EXIT_SUCCESS;
}