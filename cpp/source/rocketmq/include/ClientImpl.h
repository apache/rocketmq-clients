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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <system_error>

#include "Client.h"
#include "ClientConfig.h"
#include "ClientManager.h"
#include "ClientResourceBundle.h"
#include "InvocationContext.h"
#include "MessageExt.h"
#include "NameServerResolver.h"
#include "OpencensusHandler.h"
#include "RpcClient.h"
#include "Session.h"
#include "TelemetryBidiReactor.h"
#include "absl/strings/string_view.h"
#include "rocketmq/MessageListener.h"
#include "rocketmq/State.h"

ROCKETMQ_NAMESPACE_BEGIN

class ClientImpl : virtual public Client {
public:
  explicit ClientImpl(absl::string_view group_name);

  /**
   * @brief Allow assigning client manager for test purpose only.
   *
   * @param client_manager
   */
  void clientManager(std::shared_ptr<ClientManager> client_manager) {
    client_manager_ = std::move(client_manager);
  }

  virtual void start();

  virtual void shutdown();

  void getRouteFor(const std::string& topic, const std::function<void(const std::error_code&, TopicRouteDataPtr)>& cb)
      LOCKS_EXCLUDED(inflight_route_requests_mtx_, topic_route_table_mtx_);

  /**
   * Gather collection of endpoints that are reachable from latest topic route
   * table.
   *
   * @param endpoints
   */
  void endpointsInUse(absl::flat_hash_set<std::string>& endpoints) override LOCKS_EXCLUDED(topic_route_table_mtx_);

  void heartbeat() override;

  bool active() override {
    State state = state_.load(std::memory_order_relaxed);
    return State::STARTING == state || State::STARTED == state;
  }

  void onRemoteEndpointRemoval(const std::vector<std::string>& hosts) override LOCKS_EXCLUDED(isolated_endpoints_mtx_);

  void schedule(const std::string& task_name, const std::function<void(void)>& task,
                std::chrono::milliseconds delay) override;

  void createSession(const std::string& target, bool verify) override;

  void withNameServerResolver(std::shared_ptr<NameServerResolver> name_server_resolver) {
    name_server_resolver_ = std::move(name_server_resolver);
  }

  void withCredentialsProvider(std::shared_ptr<CredentialsProvider> credentials_provider) override {
    client_config_.credentials_provider = std::move(credentials_provider);
  }

  void withRequestTimeout(std::chrono::milliseconds request_timeout) {
    client_config_.request_timeout = absl::FromChrono(request_timeout);
  }

  void withSsl(bool with_ssl) {
    client_config_.withSsl = with_ssl;
  }

  /**
   * Expose for test purpose only.
   */
  void state(State state) {
    state_.store(state, std::memory_order_relaxed);
  }

  void verify(MessageConstSharedPtr message, std::function<void(TelemetryCommand)> cb) override;

  void recoverOrphanedTransaction(MessageConstSharedPtr message) override;

  virtual void doRecoverOrphanedTransaction(MessageConstSharedPtr message);

  ClientConfig& config() override {
    return client_config_;
  }

  std::shared_ptr<ClientManager> manager() const override {
    return client_manager_;
  }

  rmq::Settings clientSettings() override;

  virtual void buildClientSettings(rmq::Settings& settings) {
  }

  void registerTracingSampler(TracingSamplerProvider *provider) {
    if (provider) {
      client_config_.sampler_ = provider->tracingSampler();
    }
  }

  std::chrono::milliseconds backoff(std::size_t attempt) {
    return std::chrono::milliseconds(client_config_.backoff_policy.backoff(attempt));
  }

protected:
  ClientConfig client_config_;

  ClientManagerPtr client_manager_;

  std::atomic<State> state_;

  absl::flat_hash_map<std::string, TopicRouteDataPtr> topic_route_table_ GUARDED_BY(topic_route_table_mtx_);
  absl::Mutex topic_route_table_mtx_ ACQUIRED_AFTER(inflight_route_requests_mtx_); // protects topic_route_table_

  absl::flat_hash_map<std::string, std::vector<std::function<void(const std::error_code&, const TopicRouteDataPtr&)>>>
      inflight_route_requests_ GUARDED_BY(inflight_route_requests_mtx_);
  absl::Mutex inflight_route_requests_mtx_ ACQUIRED_BEFORE(topic_route_table_mtx_); // Protects inflight_route_requests_
  static const char* UPDATE_ROUTE_TASK_NAME;
  std::uint32_t route_update_handle_{0};

  static const char* TELEMETRY_TASK_NAME;
  std::uint32_t telemetry_handle_{0};

  void syncClientSettings() LOCKS_EXCLUDED(session_map_mtx_);

  // Set Name Server Resolver
  std::shared_ptr<NameServerResolver> name_server_resolver_;

  absl::flat_hash_map<std::string, absl::Time> multiplexing_requests_;
  absl::Mutex multiplexing_requests_mtx_;

  absl::flat_hash_set<std::string> isolated_endpoints_ GUARDED_BY(isolated_endpoints_mtx_);
  absl::Mutex isolated_endpoints_mtx_;

  absl::flat_hash_map<std::string, std::unique_ptr<Session>> session_map_ GUARDED_BY(session_map_mtx_);
  absl::Mutex session_map_mtx_;

  virtual void topicsOfInterest(std::vector<std::string> &topics) {
  }

  void updateRouteInfo() LOCKS_EXCLUDED(topic_route_table_mtx_);

  /**
   * Sub-class is supposed to inherit from std::enable_shared_from_this.
   */
  virtual std::shared_ptr<ClientImpl> self() = 0;

  virtual void prepareHeartbeatData(HeartbeatRequest& request) = 0;

  /**
   * @brief Execute transaction-state-checker to commit or roll-back the orphan transactional message.
   *
   * It is no-op by default and Producer-subclass is supposed to override it.
   *
   * @param message
   */
  virtual void onOrphanedTransactionalMessage(MessageConstSharedPtr message) {
  }

  virtual void onVerifyMessage(MessageConstSharedPtr message, std::function<void(TelemetryCommand)> cb);

  void notifyClientTermination() override;

  void notifyClientTermination(const NotifyClientTerminationRequest& request);

  const std::string& resourceNamespace() const {
    return client_config_.resource_namespace;
  }

  absl::Duration requestTimeout() const {
    return client_config_.request_timeout;
  }

  rmq::Endpoints accessPoint();

private:
  /**
   * This is a low-level API that fetches route data from name server through
   * gRPC unary request/response. Once request/response is completed, either
   * timeout or response arrival in time, callback would get invoked.
   * @param topic
   * @param cb
   */
  void fetchRouteFor(const std::string& topic,
                     const std::function<void(const std::error_code&, const TopicRouteDataPtr&)>& cb);

  /**
   * Callback to execute once route data is fetched from name server.
   * @param topic
   * @param route
   */
  void onTopicRouteReady(const std::string& topic, const std::error_code& ec, const TopicRouteDataPtr& route)
      LOCKS_EXCLUDED(inflight_route_requests_mtx_);

  /**
   * Update local cache for the topic. Note, route differences are logged in
   * INFO level since route bears fundamental importance.
   *
   * @param topic
   * @param route
   */
  void updateRouteCache(const std::string& topic, const std::error_code& ec, const TopicRouteDataPtr& route)
      LOCKS_EXCLUDED(topic_route_table_mtx_);

  void doVerify(std::string target, std::string command_id, MessageConstPtr message);

  std::string metricServiceEndpoint() const;
};

/**
 * ClientID is required to remain unique in the following scenarios:
 *
 * 1. Create multiple clients;
 * 2. Restart the client program in container deployments;
 *
 * @return Unique Client-ID
 */
std::string clientId();

ROCKETMQ_NAMESPACE_END