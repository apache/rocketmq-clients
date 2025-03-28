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
#include "RpcClientImpl.h"

#include <functional>
#include <sstream>
#include <thread>

#include "ClientManager.h"
#include "ReceiveMessageStreamReader.h"
#include "RpcClient.h"
#include "TelemetryBidiReactor.h"
#include "TlsHelper.h"

ROCKETMQ_NAMESPACE_BEGIN

using ClientContext = grpc::ClientContext;

void RpcClientImpl::asyncCallback(std::weak_ptr<RpcClient> client, BaseInvocationContext* invocation_context,
                                  grpc::Status status) {

  invocation_context->status = std::move(status);
  std::shared_ptr<RpcClient> stub = client.lock();
  if (!stub) {
    SPDLOG_WARN("RpcClient has destructed. Response Ignored");
    // TODO: execute orphan callback in event-loop thread?
    // invocation_context->onCompletion(false);
    // or
    delete invocation_context;
    return;
  }

  std::weak_ptr<ClientManager> client_manager = stub->clientManager();
  std::shared_ptr<ClientManager> manager = client_manager.lock();
  if (!manager) {
    SPDLOG_WARN("ClientManager has destructed. Response ignored");
    // TODO: execute orphan callback in event-loop thread?
    // invocation_context->onCompletion(false);
    // or
    delete invocation_context;
  }

  auto task = [invocation_context, client] {
    auto ptr = client.lock();
    if (!ptr) {
      // RPC client should have destructed.
      return;
    }
    invocation_context->onCompletion(invocation_context->status.ok());
  };

  // Execute business post-processing in callback thread pool.
  manager->submit(task);
}

void RpcClientImpl::asyncQueryRoute(const QueryRouteRequest& request,
                                    InvocationContext<QueryRouteResponse>* invocation_context) {
  std::weak_ptr<RpcClient> rpc_client(shared_from_this());
  auto callback = std::bind(&RpcClientImpl::asyncCallback, rpc_client, invocation_context, std::placeholders::_1);
  stub_->async()->QueryRoute(&invocation_context->context, &request, &invocation_context->response, callback);
}

void RpcClientImpl::asyncSend(const SendMessageRequest& request,
                              InvocationContext<SendMessageResponse>* invocation_context) {
  std::weak_ptr<RpcClient> rpc_client(shared_from_this());
  auto callback = std::bind(&RpcClientImpl::asyncCallback, rpc_client, invocation_context, std::placeholders::_1);
  stub_->async()->SendMessage(&invocation_context->context, &request, &invocation_context->response, callback);
}

void RpcClientImpl::asyncQueryAssignment(const QueryAssignmentRequest& request,
                                         InvocationContext<QueryAssignmentResponse>* invocation_context) {
  std::weak_ptr<RpcClient> rpc_client(shared_from_this());
  auto callback = std::bind(&RpcClientImpl::asyncCallback, rpc_client, invocation_context, std::placeholders::_1);
  stub_->async()->QueryAssignment(&invocation_context->context, &request, &invocation_context->response, callback);
}

void RpcClientImpl::asyncReceive(const ReceiveMessageRequest& request, std::unique_ptr<ReceiveMessageContext> context) {
  new ReceiveMessageStreamReader(client_manager_, stub_.get(), peer_address_, request, std::move(context));
}

void RpcClientImpl::asyncAck(const AckMessageRequest& request,
                             InvocationContext<AckMessageResponse>* invocation_context) {
  std::weak_ptr<RpcClient> rpc_client(shared_from_this());
  auto callback = std::bind(&RpcClientImpl::asyncCallback, rpc_client, invocation_context, std::placeholders::_1);
  stub_->async()->AckMessage(&invocation_context->context, &request, &invocation_context->response, callback);
}

void RpcClientImpl::asyncChangeInvisibleDuration(
    const ChangeInvisibleDurationRequest& request,
    InvocationContext<ChangeInvisibleDurationResponse>* invocation_context) {

  std::weak_ptr<RpcClient> rpc_client(shared_from_this());
  auto callback = std::bind(&RpcClientImpl::asyncCallback, rpc_client, invocation_context, std::placeholders::_1);

  stub_->async()->ChangeInvisibleDuration(&invocation_context->context, &request, &invocation_context->response,
                                          callback);
}

void RpcClientImpl::asyncHeartbeat(const HeartbeatRequest& request,
                                   InvocationContext<HeartbeatResponse>* invocation_context) {
  std::weak_ptr<RpcClient> rpc_client(shared_from_this());
  auto callback = std::bind(&RpcClientImpl::asyncCallback, rpc_client, invocation_context, std::placeholders::_1);
  stub_->async()->Heartbeat(&invocation_context->context, &request, &invocation_context->response, callback);
}

void RpcClientImpl::asyncEndTransaction(const EndTransactionRequest& request,
                                        InvocationContext<EndTransactionResponse>* invocation_context) {
  std::weak_ptr<RpcClient> rpc_client(shared_from_this());
  auto callback = std::bind(&RpcClientImpl::asyncCallback, rpc_client, invocation_context, std::placeholders::_1);
  stub_->async()->EndTransaction(&invocation_context->context, &request, &invocation_context->response, callback);
}

void RpcClientImpl::asyncRecallMessage(const RecallMessageRequest& request,
                                       InvocationContext<RecallMessageResponse>* invocation_context) {
  std::weak_ptr<RpcClient> rpc_client(shared_from_this());
  auto callback = std::bind(&RpcClientImpl::asyncCallback, rpc_client, invocation_context, std::placeholders::_1);
  stub_->async()->RecallMessage(&invocation_context->context, &request, &invocation_context->response, callback);
}

bool RpcClientImpl::ok() const {
  return channel_ && grpc_connectivity_state::GRPC_CHANNEL_SHUTDOWN != channel_->GetState(false);
}

void RpcClientImpl::addMetadata(grpc::ClientContext& context,
                                const absl::flat_hash_map<std::string, std::string>& metadata) {
  for (const auto& entry : metadata) {
    context.AddMetadata(entry.first, entry.second);
  }
}

bool RpcClientImpl::needHeartbeat() {
  return need_heartbeat_;
}

void RpcClientImpl::needHeartbeat(bool need_heartbeat) {
  need_heartbeat_ = need_heartbeat;
}

std::shared_ptr<TelemetryBidiReactor> RpcClientImpl::asyncTelemetry(std::weak_ptr<Client> client) {
  return std::make_shared<TelemetryBidiReactor>(client, stub_.get(), peer_address_);
}

grpc::Status RpcClientImpl::notifyClientTermination(grpc::ClientContext* context,
                                                    const NotifyClientTerminationRequest& request,
                                                    NotifyClientTerminationResponse* response) {
  return stub_->NotifyClientTermination(context, request, response);
}

void RpcClientImpl::asyncForwardMessageToDeadLetterQueue(
    const ForwardMessageToDeadLetterQueueRequest& request,
    InvocationContext<ForwardMessageToDeadLetterQueueResponse>* invocation_context) {
  std::weak_ptr<RpcClient> rpc_client(shared_from_this());
  auto callback = std::bind(&RpcClientImpl::asyncCallback, rpc_client, invocation_context, std::placeholders::_1);
  stub_->async()->ForwardMessageToDeadLetterQueue(&invocation_context->context, &request, &invocation_context->response,
                                                  callback);
}

std::weak_ptr<ClientManager> RpcClientImpl::clientManager() {
  return client_manager_;
}

ROCKETMQ_NAMESPACE_END