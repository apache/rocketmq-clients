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
#include "AdminServiceImpl.h"
#include "spdlog/spdlog.h"

ROCKETMQ_NAMESPACE_BEGIN

namespace admin {

Status applyChange(const rmq::ChangeLogLevelRequest* request) {
  auto logger = spdlog::get("rocketmq_logger");
  if (!logger) {
    return Status(grpc::StatusCode::INTERNAL, "rocketmq_logger is not registered");
  }

  auto level = request->level();
  switch (level) {
    case rmq::ChangeLogLevelRequest_Level_TRACE:
      logger->set_level(spdlog::level::trace);
      break;
    case rmq::ChangeLogLevelRequest_Level_DEBUG:
      logger->set_level(spdlog::level::debug);
      break;
    case rmq::ChangeLogLevelRequest_Level_INFO:
      logger->set_level(spdlog::level::info);
      break;
    case rmq::ChangeLogLevelRequest_Level_WARN:
      logger->set_level(spdlog::level::warn);
      break;
    case rmq::ChangeLogLevelRequest_Level_ERROR:
      logger->set_level(spdlog::level::err);
      break;
    default:
      logger->set_level(spdlog::level::info);
      break;
  }
  return grpc::Status::OK;
}

Status AdminServiceImpl::ChangeLogLevel(ServerContext* context, const rmq::ChangeLogLevelRequest* request,
                                        rmq::ChangeLogLevelResponse* reply) {
  Status status = applyChange(request);
  if (status.ok()) {
    reply->set_remark("OK");
  }
  return status;
}

} // namespace admin

ROCKETMQ_NAMESPACE_END