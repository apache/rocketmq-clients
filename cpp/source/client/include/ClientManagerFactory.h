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

#include <string>
#include <thread>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"

#include "ClientConfig.h"
#include "ClientManager.h"
#include "rocketmq/AdminServer.h"

ROCKETMQ_NAMESPACE_BEGIN

class ClientManagerFactory {
public:
  static ClientManagerFactory& getInstance();

  ClientManagerPtr getClientManager(const ClientConfig& client_config) LOCKS_EXCLUDED(client_manager_table_mtx_);

  // For test purpose only
  void addClientManager(const std::string& resource_namespace, const ClientManagerPtr& client_manager)
      LOCKS_EXCLUDED(client_manager_table_mtx_);

private:
  ClientManagerFactory();

  virtual ~ClientManagerFactory();

  /**
   * Client Id --> Client Instance
   */
  absl::flat_hash_map<std::string, std::weak_ptr<ClientManager>>
      client_manager_table_ GUARDED_BY(client_manager_table_mtx_);
  absl::Mutex client_manager_table_mtx_; // protects client_manager_table_

  rocketmq::admin::AdminServer& admin_server_;
};

ROCKETMQ_NAMESPACE_END