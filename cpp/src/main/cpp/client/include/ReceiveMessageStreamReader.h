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

#include <chrono>
#include <functional>
#include <memory>
#include <system_error>

#include "MessageExt.h"
#include "ReceiveMessageCallback.h"
#include "ReceiveMessageContext.h"
#include "grpcpp/client_context.h"

#include "Client.h"
#include "ClientManager.h"
#include "Protocol.h"
#include "ReceiveMessageContext.h"
#include "ReceiveMessageResult.h"
#include "rocketmq/ErrorCode.h"

ROCKETMQ_NAMESPACE_BEGIN

class ReceiveMessageStreamReader : public grpc::ClientReadReactor<rmq::ReceiveMessageResponse> {
public:
  ReceiveMessageStreamReader(std::weak_ptr<ClientManager> client_manager,
                             rmq::MessagingService::Stub* stub,
                             std::string peer_address,
                             rmq::ReceiveMessageRequest request,
                             std::unique_ptr<ReceiveMessageContext> context);

  void OnReadDone(bool ok) override;

  void OnDone(const grpc::Status& s) override;

private:
  std::weak_ptr<ClientManager> client_manager_;
  rmq::MessagingService::Stub* stub_;
  std::string peer_address_;
  rmq::ReceiveMessageRequest request_;
  std::unique_ptr<ReceiveMessageContext> context_;
  rmq::ReceiveMessageResponse response_;
  grpc::ClientContext client_context_;
  grpc::Status status_;
  std::error_code ec_;
  ReceiveMessageResult result_;
};

ROCKETMQ_NAMESPACE_END