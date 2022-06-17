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
#include "ClientManagerFactory.h"
#include "ClientManagerImpl.h"

ROCKETMQ_NAMESPACE_BEGIN

ClientManagerFactory& ClientManagerFactory::getInstance() {
  static ClientManagerFactory instance;
  return instance;
}

ClientManagerPtr ClientManagerFactory::getClientManager(const ClientConfig& client_config) {
  {
    absl::MutexLock lock(&client_manager_table_mtx_);
    auto search = client_manager_table_.find(client_config.resource_namespace);
    if (search != client_manager_table_.end()) {
      ClientManagerPtr client_manager = search->second.lock();
      if (client_manager) {
        SPDLOG_DEBUG("Re-use existing client_manager[resource_namespace={}]", client_config.resource_namespace);
        return client_manager;
      } else {
        client_manager_table_.erase(client_config.resource_namespace);
      }
    }
    ClientManagerPtr client_manager = std::make_shared<ClientManagerImpl>(client_config.resource_namespace);
    std::weak_ptr<ClientManager> client_instance_weak_ptr(client_manager);
    client_manager_table_.insert_or_assign(client_config.resource_namespace, client_instance_weak_ptr);
    SPDLOG_INFO("Created a new client manager[resource_namespace={}]", client_config.resource_namespace);
    return client_manager;
  }
}

void ClientManagerFactory::addClientManager(const std::string& resource_namespace,
                                            const ClientManagerPtr& client_manager) {
  absl::MutexLock lk(&client_manager_table_mtx_);
  client_manager_table_.insert_or_assign(resource_namespace, std::weak_ptr<ClientManager>(client_manager));
}

ClientManagerFactory::ClientManagerFactory() : admin_server_(admin::AdminFacade::getServer()) {
  admin_server_.start();
}

ClientManagerFactory::~ClientManagerFactory() {
  admin_server_.stop();
}

ROCKETMQ_NAMESPACE_END