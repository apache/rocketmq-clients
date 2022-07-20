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

#include <memory>

#include "Client.h"
#include "RpcClient.h"
#include "Session.h"
#include "TelemetryBidiReactor.h"

ROCKETMQ_NAMESPACE_BEGIN

class SessionImpl : public Session {
public:
  SessionImpl(std::weak_ptr<Client> client, std::shared_ptr<RpcClient> rpc_client);

  ~SessionImpl() override;

  bool await() override;

private:
  std::weak_ptr<Client> client_;
  std::shared_ptr<RpcClient> rpc_client_;
  // TODO: use unique_ptr
  std::shared_ptr<TelemetryBidiReactor> telemetry_;

  void syncSettings() override;
};

ROCKETMQ_NAMESPACE_END