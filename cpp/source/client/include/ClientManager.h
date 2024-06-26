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

#include "Client.h"
#include "Metadata.h"
#include "ReceiveMessageCallback.h"
#include "RpcClient.h"
#include "Scheduler.h"
#include "SendResultCallback.h"
#include "TopicRouteData.h"
#include "rocketmq/State.h"

ROCKETMQ_NAMESPACE_BEGIN

/**
 * @brief SessionManager
 */
class ClientManager {
public:
  virtual ~ClientManager() = default;

  virtual void start() = 0;

  virtual void shutdown() = 0;

  virtual SchedulerSharedPtr getScheduler() = 0;

  virtual std::shared_ptr<RpcClient> getRpcClient(const std::string& target_host, bool need_heartbeat) = 0;

  /**
   * @brief Create a Channel object
   *
   * @param target_host gRPC naming targets, following https://github.com/grpc/grpc/blob/master/doc/naming.md
   * @return std::shared_ptr<grpc::Channel>
   */
  virtual std::shared_ptr<grpc::Channel> createChannel(const std::string& target_host) = 0;

  virtual void resolveRoute(const std::string& target_host, const Metadata& metadata, const QueryRouteRequest& request,
                            std::chrono::milliseconds timeout,
                            const std::function<void(const std::error_code&, const TopicRouteDataPtr& ptr)>& cb) = 0;

  virtual void heartbeat(const std::string& target_host, const Metadata& metadata, const HeartbeatRequest& request,
                         std::chrono::milliseconds timeout,
                         const std::function<void(const std::error_code&, const HeartbeatResponse&)>& cb) = 0;

  virtual MessageConstSharedPtr wrapMessage(const rmq::Message&) = 0;

  virtual void ack(const std::string& target_host, const Metadata& metadata, const AckMessageRequest& request,
                   std::chrono::milliseconds timeout, const std::function<void(const std::error_code&)>& cb) = 0;

  virtual void changeInvisibleDuration(const std::string& target_host, const Metadata& metadata,
                                       const ChangeInvisibleDurationRequest&, std::chrono::milliseconds timeout,
                                       const std::function<void(const std::error_code&)>&) = 0;

  virtual void forwardMessageToDeadLetterQueue(
      const std::string& target_host, const Metadata& metadata, const ForwardMessageToDeadLetterQueueRequest& request,
      std::chrono::milliseconds timeout,
      const std::function<void(const std::error_code&)>& cb) = 0;

  virtual void endTransaction(const std::string& target_host, const Metadata& metadata,
                              const EndTransactionRequest& request, std::chrono::milliseconds timeout,
                              const std::function<void(const std::error_code&, const EndTransactionResponse&)>& cb) = 0;

  virtual void addClientObserver(std::weak_ptr<Client> client) = 0;

  virtual void
  queryAssignment(const std::string& target, const Metadata& metadata, const QueryAssignmentRequest& request,
                  std::chrono::milliseconds timeout,
                  const std::function<void(const std::error_code&, const QueryAssignmentResponse&)>& cb) = 0;

  virtual void receiveMessage(const std::string& target, const Metadata& metadata, const ReceiveMessageRequest& request,
                              std::chrono::milliseconds timeout, ReceiveMessageCallback callback) = 0;

  virtual bool send(const std::string& target_host,
                    const Metadata& metadata,
                    SendMessageRequest& request,
                    SendResultCallback cb) = 0;

  virtual std::error_code notifyClientTermination(const std::string& target_host, const Metadata& metadata,
                                                  const NotifyClientTerminationRequest& request,
                                                  std::chrono::milliseconds timeout) = 0;

  virtual State state() const = 0;

  virtual void submit(std::function<void()> task) = 0;
};

using ClientManagerPtr = std::shared_ptr<ClientManager>;

ROCKETMQ_NAMESPACE_END