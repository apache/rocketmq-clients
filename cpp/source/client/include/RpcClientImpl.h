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
#include "ClientManager.h"
#include "ReceiveMessageContext.h"
#include "RpcClient.h"
#include "absl/container/flat_hash_map.h"

ROCKETMQ_NAMESPACE_BEGIN

class RpcClientImpl : public RpcClient, public std::enable_shared_from_this<RpcClientImpl> {
public:
  RpcClientImpl(std::weak_ptr<ClientManager> client_manager, std::shared_ptr<Channel> channel, std::string peer_address,
                bool need_heartbeat = true)
      : client_manager_(client_manager), channel_(std::move(channel)), peer_address_(std::move(peer_address)),
        stub_(rmq::MessagingService::NewStub(channel_)), need_heartbeat_(need_heartbeat) {
  }

  RpcClientImpl(const RpcClientImpl&) = delete;

  RpcClientImpl& operator=(const RpcClientImpl&) = delete;

  ~RpcClientImpl() override = default;

  const std::string& remoteAddress() const override {
    return peer_address_;
  }

  void asyncQueryRoute(const QueryRouteRequest& request,
                       InvocationContext<QueryRouteResponse>* invocation_context) override;

  void asyncSend(const SendMessageRequest& request,
                 InvocationContext<SendMessageResponse>* invocation_context) override;

  void asyncQueryAssignment(const QueryAssignmentRequest& request,
                            InvocationContext<QueryAssignmentResponse>* invocation_context) override;

  void asyncReceive(const ReceiveMessageRequest& request, std::unique_ptr<ReceiveMessageContext> context) override;

  void asyncAck(const AckMessageRequest& request, InvocationContext<AckMessageResponse>* invocation_context) override;

  void asyncChangeInvisibleDuration(const ChangeInvisibleDurationRequest& request,
                                    InvocationContext<ChangeInvisibleDurationResponse>*) override;

  void asyncHeartbeat(const HeartbeatRequest& request,
                      InvocationContext<HeartbeatResponse>* invocation_context) override;

  void asyncEndTransaction(const EndTransactionRequest& request,
                           InvocationContext<EndTransactionResponse>* invocation_context) override;

  void asyncRecallMessage(const RecallMessageRequest& request,
                          InvocationContext<RecallMessageResponse>* invocation_context) override;

  std::shared_ptr<TelemetryBidiReactor> asyncTelemetry(std::weak_ptr<Client> client) override;

  void asyncForwardMessageToDeadLetterQueue(
      const ForwardMessageToDeadLetterQueueRequest& request,
      InvocationContext<ForwardMessageToDeadLetterQueueResponse>* invocation_context) override;

  grpc::Status notifyClientTermination(grpc::ClientContext* context, const NotifyClientTerminationRequest& request,
                                       NotifyClientTerminationResponse* response) override;

  std::weak_ptr<ClientManager> clientManager() override;

  bool needHeartbeat() override;

  void needHeartbeat(bool need_heartbeat) override;

  bool ok() const override;

private:
  static void addMetadata(grpc::ClientContext& context, const absl::flat_hash_map<std::string, std::string>& metadata);

  static void asyncCallback(std::weak_ptr<RpcClient> client, BaseInvocationContext* invocation_context,
                            grpc::Status status);

  std::weak_ptr<ClientManager> client_manager_;
  std::shared_ptr<Channel> channel_;
  std::string peer_address_;
  std::unique_ptr<rmq::MessagingService::Stub> stub_;
  std::chrono::milliseconds connect_timeout_{3000};
  bool need_heartbeat_{true};
};

using RpcClientSharedPtr = std::shared_ptr<RpcClient>;

ROCKETMQ_NAMESPACE_END