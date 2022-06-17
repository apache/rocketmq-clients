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

#include <functional>

#include "ClientConfig.h"
#include "RpcClient.h"
#include "absl/container/flat_hash_set.h"
#include "rocketmq/CredentialsProvider.h"

ROCKETMQ_NAMESPACE_BEGIN

class Client {
public:
  virtual ~Client() = default;

  virtual rmq::Settings clientSettings() = 0;

  virtual ClientConfig& config() = 0;

  virtual void endpointsInUse(absl::flat_hash_set<std::string>& endpoints) = 0;

  virtual void heartbeat() = 0;

  virtual bool active() = 0;

  virtual void onRemoteEndpointRemoval(const std::vector<std::string>&) = 0;

  virtual void schedule(const std::string& task_name, const std::function<void(void)>& task,
                        std::chrono::milliseconds delay) = 0;

  /**
   * @brief Create a Session object
   *
   * @param target Remote endpoint address in form of gRPC naming convention.
   * @param verify Verify presence of target in route table before creating session.
   */
  virtual void createSession(const std::string& target, bool verify) = 0;

  virtual void verify(MessageConstSharedPtr message, std::function<void(TelemetryCommand)>) = 0;

  virtual void recoverOrphanedTransaction(MessageConstSharedPtr message) = 0;

  virtual void notifyClientTermination() = 0;

  virtual void withCredentialsProvider(std::shared_ptr<CredentialsProvider> credentials_provider) = 0;

  virtual std::shared_ptr<ClientManager> manager() const = 0;
};

ROCKETMQ_NAMESPACE_END