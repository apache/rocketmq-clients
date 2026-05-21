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
#include <iostream>
#include <memory>
#include <string>

#include "Protocol.h"
#include "ReceiveMessageContext.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "grpcpp/grpcpp.h"

ROCKETMQ_NAMESPACE_BEGIN

using Channel = grpc::Channel;
using Status = grpc::Status;
using CompletionQueue = grpc::CompletionQueue;

class Client;
class ClientManager;
class TelemetryBidiReactor;

/**
 * @brief A RpcClient represents a session between client and a remote broker.
 *
 */
class RpcClient {
public:
  RpcClient() = default;

  virtual ~RpcClient() = default;

  virtual const std::string& remoteAddress() const = 0;

  virtual void asyncQueryRoute(const QueryRouteRequest& request,
                               InvocationContext<QueryRouteResponse>* invocation_context) = 0;

  virtual void asyncSend(const SendMessageRequest& request,
                         InvocationContext<SendMessageResponse>* invocation_context) = 0;

  virtual void asyncQueryAssignment(const QueryAssignmentRequest& request,
                                    InvocationContext<QueryAssignmentResponse>* invocation_context) = 0;

  virtual void asyncReceive(const ReceiveMessageRequest& request, std::unique_ptr<ReceiveMessageContext> context) = 0;

  virtual void asyncAck(const AckMessageRequest& request,
                        InvocationContext<AckMessageResponse>* invocation_context) = 0;

  virtual void asyncChangeInvisibleDuration(const ChangeInvisibleDurationRequest& request,
                                            InvocationContext<ChangeInvisibleDurationResponse>*) = 0;

  virtual void asyncHeartbeat(const HeartbeatRequest& request,
                              InvocationContext<HeartbeatResponse>* invocation_context) = 0;

  virtual void asyncEndTransaction(const EndTransactionRequest& request,
                                   InvocationContext<EndTransactionResponse>* invocation_context) = 0;

  virtual void asyncRecallMessage(const RecallMessageRequest& request,
                                   InvocationContext<RecallMessageResponse>* invocation_context) = 0;

  virtual std::shared_ptr<TelemetryBidiReactor> asyncTelemetry(std::weak_ptr<Client> client) = 0;

  virtual void asyncForwardMessageToDeadLetterQueue(
      const ForwardMessageToDeadLetterQueueRequest& request,
      InvocationContext<ForwardMessageToDeadLetterQueueResponse>* invocation_context) = 0;

  virtual grpc::Status notifyClientTermination(grpc::ClientContext* context,
                                               const NotifyClientTerminationRequest& request,
                                               NotifyClientTerminationResponse* response) = 0;

  virtual std::weak_ptr<ClientManager> clientManager() = 0;

  /**
   * Indicate if heartbeat is required.
   * @return true if periodic heartbeat is required; false otherwise.
   */
  virtual bool needHeartbeat() = 0;

  virtual void needHeartbeat(bool need_heartbeat) = 0;

  /**
   * Indicate if current client connection state is OK or recoverable.
   *
   * @return true if underlying connection is OK or recoverable; false otherwise.
   */
  virtual bool ok() const = 0;
};

ROCKETMQ_NAMESPACE_END