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

#include "SessionImpl.h"

#include "rocketmq/Logger.h"
#include "spdlog/spdlog.h"

ROCKETMQ_NAMESPACE_BEGIN

SessionImpl::SessionImpl(std::weak_ptr<Client> client, std::shared_ptr<RpcClient> rpc_client)
    : client_(client), rpc_client_(rpc_client) {
  telemetry_ = rpc_client->asyncTelemetry(client_);
  syncSettings();
}

bool SessionImpl::await() {
  return telemetry_->awaitApplyingSettings();
}

void SessionImpl::syncSettings() {
  auto ptr = client_.lock();
  SPDLOG_INFO("Request client settings to {}", rpc_client_->remoteAddress());
  TelemetryCommand command;
  command.mutable_settings()->CopyFrom(ptr->clientSettings());
  telemetry_->write(command);
}

SessionImpl::~SessionImpl() {
  telemetry_->close();
  SPDLOG_DEBUG("Session for {} destructed", rpc_client_->remoteAddress());
}

ROCKETMQ_NAMESPACE_END