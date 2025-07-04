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
#include "ClientImpl.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <functional>
#include <memory>
#include <string>
#include <system_error>
#include <utility>

#include "ClientManagerImpl.h"
#include "InvocationContext.h"
#include "MessageExt.h"
#include "MixAll.h"
#include "NamingScheme.h"
#include "SessionImpl.h"
#include "Signature.h"
#include "StdoutHandler.h"
#include "UtilAll.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "fmt/format.h"
#include "opencensus/stats/stats.h"
#include "spdlog/spdlog.h"

ROCKETMQ_NAMESPACE_BEGIN

ClientImpl::ClientImpl(absl::string_view group_name) : state_(State::CREATED) {
  client_config_.subscriber.group.set_name(std::string(group_name.data(), group_name.length()));
}

rmq::Endpoints ClientImpl::accessPoint() {
  std::string    endpoints = name_server_resolver_->resolve();
  rmq::Endpoints access_point;

  absl::string_view host_port;
  if (absl::StartsWith(endpoints, NamingScheme::IPv4Prefix)) {
    access_point.set_scheme(rmq::AddressScheme::IPv4);
    host_port = absl::StripPrefix(endpoints, NamingScheme::IPv4Prefix);
  } else if (absl::StartsWith(endpoints, NamingScheme::IPv6Prefix)) {
    access_point.set_scheme(rmq::AddressScheme::IPv6);
    host_port = absl::StripPrefix(endpoints, NamingScheme::IPv6Prefix);
  } else {
    access_point.set_scheme(rmq::AddressScheme::DOMAIN_NAME);
    host_port = absl::StripPrefix(endpoints, NamingScheme::DnsPrefix);
  }

  std::vector<std::string> pairs = absl::StrSplit(host_port, absl::ByAnyChar(";,"), absl::SkipWhitespace());
  // Now endpoint is in form of host:port
  for (auto& endpoint : pairs) {
    std::reverse(endpoint.begin(), endpoint.end());
    std::vector<std::string> segments = absl::StrSplit(endpoint, absl::MaxSplits(':', 1));
    for (auto& segment : segments) {
      std::reverse(segment.begin(), segment.end());
    }
    if (segments.size() != 2) {
      continue;
    }

    std::int32_t port;
    if (!absl::SimpleAtoi(segments[0], &port)) {
      // Failed to parse port
      continue;
    }

    auto addr = new rmq::Address();
    addr->set_host(segments[1]);
    addr->set_port(port);
    access_point.mutable_addresses()->AddAllocated(addr);
  }
  return access_point;
}

void ClientImpl::start() {
  State expected = CREATED;
  if (!state_.compare_exchange_strong(expected, State::STARTING)) {
    SPDLOG_ERROR("Attempt to start ClientImpl failed. Expecting: {} Actual: {}", State::CREATED,
                 state_.load(std::memory_order_relaxed));
    return;
  }

  if (!name_server_resolver_) {
    SPDLOG_ERROR("No name server resolver is configured.");
    abort();
  }
  name_server_resolver_->start();

  client_config_.client_id = clientId();
  if (!client_manager_) {
    client_manager_ = std::make_shared<ClientManagerImpl>(
        client_config_.resource_namespace, client_config_.withSsl);
    client_manager_->start();
  }

  const auto& endpoint = name_server_resolver_->resolve();
  if (endpoint.empty()) {
    SPDLOG_ERROR("Failed to resolve name server address");
    return;
  }

  while (true) {
    createSession(endpoint, false);
    {
      absl::MutexLock lk(&session_map_mtx_);
      if (session_map_.contains(endpoint) && session_map_[endpoint]->await()) {
        break;
      }
      session_map_.erase(endpoint);
    }
  }

  std::weak_ptr<ClientImpl> ptr(self());
  {
    // Query routes for topics of interest in synchronous
    std::vector<std::string> topics;
    topicsOfInterest(topics);

    auto mtx = std::make_shared<absl::Mutex>();
    auto cv = std::make_shared<absl::CondVar>();
    bool completed = false;
    for (const auto& topic : topics) {
      completed = false;
      auto callback = [&, mtx, cv](const std::error_code& ec, const TopicRouteDataPtr ptr) {
        if (ec) {
          SPDLOG_ERROR("Failed to query route for {} during starting. Cause: {}", topic, ec.message());
        }

        {
          absl::MutexLock lk(mtx.get());
          completed = true;
        }
        cv->Signal();
      };
      getRouteFor(topic, callback);
      {
        absl::MutexLock lk(mtx.get());
        if (!completed) {
          cv->Wait(mtx.get());
        }
      }
    }
  }

  auto route_update_functor = [ptr]() {
    std::shared_ptr<ClientImpl> base = ptr.lock();
    if (base) {
      base->updateRouteInfo();
    }
  };

  route_update_handle_ = client_manager_->getScheduler()->schedule(
      route_update_functor, UPDATE_ROUTE_TASK_NAME,
      std::chrono::seconds(10), std::chrono::seconds(30));

  auto telemetry_functor = [ptr]() {
    std::shared_ptr<ClientImpl> base = ptr.lock();
    if (base) {
      SPDLOG_DEBUG("Sync client settings to servers");
      base->syncClientSettings();
    }
  };

  // refer java sdk: set refresh interval to 5 minutes
  // org.apache.rocketmq.client.java.impl.ClientSessionImpl#syncSettings0
  telemetry_handle_ = client_manager_->getScheduler()->schedule(
      telemetry_functor, TELEMETRY_TASK_NAME,
      std::chrono::minutes(5), std::chrono::minutes(5));

  auto&& metric_service_endpoint = metricServiceEndpoint();
  if (!metric_service_endpoint.empty()) {
    std::weak_ptr<Client> client_weak_ptr(self());
#ifdef DEBUG_METRIC_EXPORTING
    opencensus::stats::StatsExporter::SetInterval(absl::Seconds(30));
    opencensus::stats::StatsExporter::RegisterPushHandler(absl::make_unique<StdoutHandler>());
#else
    opencensus::stats::StatsExporter::SetInterval(absl::Minutes(1));
#endif
    SPDLOG_INFO("Export client metrics to {}", metric_service_endpoint);
    opencensus::stats::StatsExporter::RegisterPushHandler(
        absl::make_unique<OpencensusHandler>(metric_service_endpoint, client_weak_ptr));
  }
}

std::string ClientImpl::metricServiceEndpoint() const {
  auto endpoints = client_config_.metric.endpoints;
  std::string service_endpoint;
  switch (endpoints.scheme()) {
    case rmq::AddressScheme::IPv4: {
      service_endpoint.append("ipv4:");
      break;
    }
    case rmq::AddressScheme::IPv6: {
      service_endpoint.append("ipv6:");
      break;
    }
    case rmq::AddressScheme::DOMAIN_NAME: {
      service_endpoint.append("dns:");
      break;
    }
    default: {
      SPDLOG_ERROR("Unknown metric address scheme");
    }
  }

  bool first = true;
  for (const auto& address : endpoints.addresses()) {
    if (!first) {
      service_endpoint.push_back(',');
    } else {
      first = false;
    }
    service_endpoint.append(address.host());
    service_endpoint.push_back(':');
    service_endpoint.append(std::to_string(address.port()));
  }
  return service_endpoint;
}

void ClientImpl::shutdown() {
  State expected = State::STOPPING;
  if (state_.compare_exchange_strong(expected, State::STOPPED)) {
    name_server_resolver_->shutdown();
    if (route_update_handle_) {
      client_manager_->getScheduler()->cancel(route_update_handle_);
    }

    if (telemetry_handle_) {
      client_manager_->getScheduler()->cancel(telemetry_handle_);
    }

    client_manager_.reset();
  } else {
    SPDLOG_ERROR("Try to shutdown ClientImpl, but its state is not as expected. Expecting: {}, Actual: {}",
                 State::STOPPING, state_.load(std::memory_order_relaxed));
  }
}

const char* ClientImpl::UPDATE_ROUTE_TASK_NAME = "route_updater";
const char* ClientImpl::TELEMETRY_TASK_NAME = "client_settings_sync";

void ClientImpl::endpointsInUse(absl::flat_hash_set<std::string>& endpoints) {
  absl::MutexLock lk(&topic_route_table_mtx_);
  for (const auto& item : topic_route_table_) {
    for (const auto& queue : item.second->messageQueues()) {
      std::string endpoint = urlOf(queue);
      if (!endpoints.contains(endpoint)) {
        endpoints.emplace(std::move(endpoint));
      }
    }
  }
}

void ClientImpl::getRouteFor(const std::string& topic,
                             const std::function<void(const std::error_code&, TopicRouteDataPtr)>& cb) {
  TopicRouteDataPtr route = nullptr;
  {
    absl::MutexLock lock(&topic_route_table_mtx_);
    if (topic_route_table_.contains(topic)) {
      route = topic_route_table_.at(topic);
    }
  }

  if (route) {
    std::error_code ec;
    cb(ec, route);
    return;
  }

  bool query_backend = true;
  {
    absl::MutexLock lk(&inflight_route_requests_mtx_);
    {
      absl::MutexLock route_table_lock(&topic_route_table_mtx_);
      if (topic_route_table_.contains(topic)) {
        route = topic_route_table_.at(topic);
        query_backend = false;
      }
    }

    if (query_backend) {
      if (inflight_route_requests_.contains(topic)) {
        inflight_route_requests_.at(topic).emplace_back(cb);
        SPDLOG_DEBUG("Would reuse prior route request for topic={}", topic);
        return;
      } else {
        std::vector<std::function<void(const std::error_code&, const TopicRouteDataPtr&)>> inflight{cb};
        inflight_route_requests_.insert({topic, inflight});
        SPDLOG_INFO("Create inflight route query cache for topic={}", topic);
      }
    }
  }

  if (!query_backend && route) {
    std::error_code ec;
    cb(ec, route);
  } else {
    fetchRouteFor(topic,
                  std::bind(&ClientImpl::onTopicRouteReady, this, topic, std::placeholders::_1, std::placeholders::_2));
  }
}

void ClientImpl::fetchRouteFor(const std::string& topic,
                               const std::function<void(const std::error_code&, const TopicRouteDataPtr&)>& cb) {
  std::string name_server = name_server_resolver_->resolve();
  if (name_server.empty()) {
    SPDLOG_WARN("No name server available");
    return;
  }

  auto callback = [this, topic, name_server, cb](const std::error_code& ec, const TopicRouteDataPtr& route) {
    if (ec) {
      SPDLOG_WARN("Failed to resolve route for topic={} from {}", topic, name_server);
      std::string name_server_changed = name_server_resolver_->resolve();
      if (!name_server_changed.empty()) {
        SPDLOG_INFO("Change current name server from {} to {}", name_server, name_server_changed);
      }
      cb(ec, nullptr);
      return;
    }

    SPDLOG_DEBUG("Apply callback of fetchRouteFor({}) since a valid route is fetched", topic);
    cb(ec, route);
  };

  QueryRouteRequest request;
  request.mutable_topic()->set_resource_namespace(client_config_.resource_namespace);
  request.mutable_topic()->set_name(topic);
  request.mutable_endpoints()->CopyFrom(accessPoint());
  absl::flat_hash_map<std::string, std::string> metadata;
  Signature::sign(client_config_, metadata);
  client_manager_->resolveRoute(name_server, metadata, request,
                                absl::ToChronoMilliseconds(client_config_.request_timeout), callback);
}

void ClientImpl::syncClientSettings() {
  absl::MutexLock lk(&session_map_mtx_);
  for (const auto& entry : session_map_) {
    entry.second->syncSettings();
  }
}

void ClientImpl::updateRouteInfo() {
  if (State::STARTED != state_.load(std::memory_order_relaxed) &&
      State::STARTING != state_.load(std::memory_order_relaxed)) {
    SPDLOG_WARN("Unexpected client instance state={}.", state_.load(std::memory_order_relaxed));
    return;
  }

  std::vector<std::string> topics;
  {
    absl::MutexLock lock(&topic_route_table_mtx_);
    for (const auto& entry : topic_route_table_) {
      topics.push_back(entry.first);
    }
  }
  topicsOfInterest(topics);

  SPDLOG_DEBUG("Query route for {}", absl::StrJoin(topics, ","));

  if (!topics.empty()) {
    for (const auto& topic : topics) {
      fetchRouteFor(
          topic, std::bind(&ClientImpl::updateRouteCache, this, topic, std::placeholders::_1, std::placeholders::_2));
    }
  }
  SPDLOG_DEBUG("Topic route info updated");
}

void ClientImpl::heartbeat() {
  absl::flat_hash_set<std::string> hosts;
  endpointsInUse(hosts);
  if (hosts.empty()) {
    SPDLOG_WARN("No hosts to send heartbeat to at present");
    return;
  }

  HeartbeatRequest request;
  prepareHeartbeatData(request);

  absl::flat_hash_map<std::string, std::string> metadata;
  Signature::sign(client_config_, metadata);

  for (const auto& target : hosts) {
    auto callback = [target](const std::error_code& ec, const HeartbeatResponse& response) {
      if (ec) {
        SPDLOG_WARN("Failed to heartbeat against {}. Cause: {}", target, ec.message());
        return;
      }
      SPDLOG_DEBUG("Heartbeat to {} OK", target);
    };
    client_manager_->heartbeat(target, metadata, request,
      absl::ToChronoMilliseconds(client_config_.request_timeout), callback);
  }
}

void ClientImpl::onTopicRouteReady(const std::string& topic, const std::error_code& ec,
                                   const TopicRouteDataPtr& route) {
  if (route) {
    SPDLOG_DEBUG("Received route data for topic={}", topic);
  }

  updateRouteCache(topic, ec, route);

  // Take all pending callbacks
  std::vector<std::function<void(const std::error_code&, const TopicRouteDataPtr&)>> pending_requests;
  {
    absl::MutexLock lk(&inflight_route_requests_mtx_);
    assert(inflight_route_requests_.contains(topic));
    auto& inflight_requests = inflight_route_requests_.at(topic);
    pending_requests.insert(pending_requests.end(), inflight_requests.begin(), inflight_requests.end());
    inflight_route_requests_.erase(topic);
  }

  SPDLOG_DEBUG("Apply cached callbacks with acquired route data for topic={}", topic);
  for (const auto& cb : pending_requests) {
    cb(ec, route);
  }
}

void ClientImpl::updateRouteCache(const std::string& topic, const std::error_code& ec, const TopicRouteDataPtr& route) {
  if (ec || !route || route->messageQueues().empty()) {
    SPDLOG_WARN("Yuck! route for {} is invalid. Cause: {}", topic, ec.message());
    return;
  }

  {
    absl::MutexLock lk(&topic_route_table_mtx_);
    if (!topic_route_table_.contains(topic)) {
      topic_route_table_.insert({topic, route});
      SPDLOG_INFO("TopicRouteData for topic={} has changed. NONE --> {}", topic, route->debugString());
    } else {
      TopicRouteDataPtr cached = topic_route_table_.at(topic);
      if (*cached != *route) {
        topic_route_table_.insert_or_assign(topic, route);
        std::string previous = cached->debugString();
        SPDLOG_INFO("TopicRouteData for topic={} has changed. {} --> {}", topic, cached->debugString(),
                    route->debugString());
      }
    }
  }

  absl::flat_hash_set<std::string> targets;
  for (const auto& message_queue : route->messageQueues()) {
    targets.insert(urlOf(message_queue));
  }

  {
    absl::MutexLock lk(&session_map_mtx_);
    for (auto it = targets.begin(); it != targets.end();) {
      if (session_map_.contains(*it)) {
        targets.erase(it++);
      } else {
        ++it;
      }
    }
  }

  if (!targets.empty()) {
    for (const auto& target : targets) {
      createSession(target, true);
    }
  }
}

rmq::Settings ClientImpl::clientSettings() {
  rmq::Settings settings;
  settings.mutable_access_point()->CopyFrom(accessPoint());

  std::int64_t seconds = absl::ToInt64Seconds(client_config_.request_timeout);
  settings.mutable_request_timeout()->set_seconds(seconds);
  std::int64_t nanos = absl::ToInt64Nanoseconds(client_config_.request_timeout - absl::Seconds(seconds));
  settings.mutable_request_timeout()->set_nanos(nanos);

  // Fill User Agent
  settings.mutable_user_agent()->set_hostname(UtilAll::hostname());
  settings.mutable_user_agent()->set_language(rmq::Language::CPP);
  settings.mutable_user_agent()->set_version(MetadataConstants::CLIENT_VERSION);
  settings.mutable_user_agent()->set_platform(MixAll::osName());

  buildClientSettings(settings);

  return settings;
}

void ClientImpl::createSession(const std::string& target, bool verify) {
  if (verify) {
    absl::flat_hash_set<std::string> endpoints;
    endpointsInUse(endpoints);
    if (!endpoints.contains(target)) {
      return;
    }
  }

  std::weak_ptr<ClientImpl> client = self();
  auto rpc_client = client_manager_->getRpcClient(target, true);
  SPDLOG_DEBUG("Create a new session for {}", target);
  auto session = absl::make_unique<SessionImpl>(client, rpc_client);
  {
    absl::MutexLock lk(&session_map_mtx_);
    session_map_.insert_or_assign(target, std::move(session));
  }
}

void ClientImpl::verify(MessageConstSharedPtr message, std::function<void(TelemetryCommand)> cb) {
  std::weak_ptr<ClientImpl> ptr(self());

  // TODO: Use capture by move if C++14 is possible
  auto task = [message, cb, ptr]() {
    auto client = ptr.lock();
    if (!client) {
      return;
    }

    client->onVerifyMessage(message, cb);
  };

  // Verify message may take a long period of time, we need to execute it in dedicated thread pool
  // such that network-IO thread will not get blocked.
  client_manager_->submit(task);
}

void ClientImpl::onVerifyMessage(MessageConstSharedPtr message, std::function<void(TelemetryCommand)> cb) {
  rmq::TelemetryCommand cmd;
  cmd.mutable_verify_message_result()->set_nonce(message->extension().nonce);
  cmd.mutable_status()->set_code(rmq::Code::NOT_IMPLEMENTED);
  cmd.mutable_status()->set_message("Unsupported Operation");
  cb(std::move(cmd));
}

void ClientImpl::recoverOrphanedTransaction(MessageConstSharedPtr message) {
  auto ptr = self();
  std::weak_ptr<ClientImpl> owner(ptr);

  auto do_recover = [message, owner]() {
    auto client = owner.lock();
    if (!client) {
      return;
    }
    client->doRecoverOrphanedTransaction(message);
  };

  // Execute orphaned transaction recovery in dedicated thread pool.
  client_manager_->submit(do_recover);
}

void ClientImpl::doRecoverOrphanedTransaction(MessageConstSharedPtr message) {
  if (!message) {
    SPDLOG_WARN("Failed to decode orphaned transaction message");
    return;
  }

  onOrphanedTransactionalMessage(message);
}

void ClientImpl::onRemoteEndpointRemoval(const std::vector<std::string>& hosts) {
  absl::MutexLock lk(&isolated_endpoints_mtx_);
  for (auto it = isolated_endpoints_.begin(); it != isolated_endpoints_.end();) {
    if (hosts.end() != std::find_if(hosts.begin(), hosts.end(), [&](const std::string& item) { return *it == item; })) {
      SPDLOG_INFO("Drop isolated-endoint[{}] as it has been removed from route table", *it);
      isolated_endpoints_.erase(it++);
    } else {
      it++;
    }
  }
}

void ClientImpl::schedule(const std::string& task_name, const std::function<void()>& task,
                          std::chrono::milliseconds delay) {
  client_manager_->getScheduler()->schedule(task, task_name, delay, std::chrono::milliseconds(0));
}

void ClientImpl::notifyClientTermination() {
  SPDLOG_WARN("Should NOT reach here. Subclass should have overridden this function.");
  std::abort();
}

void ClientImpl::notifyClientTermination(const NotifyClientTerminationRequest& request) {
  absl::flat_hash_set<std::string> endpoints;
  endpointsInUse(endpoints);

  Metadata metadata;
  Signature::sign(client_config_, metadata);

  for (const auto& endpoint : endpoints) {
    std::error_code ec = client_manager_->notifyClientTermination(
        endpoint, metadata, request,absl::ToChronoMilliseconds(client_config_.request_timeout));
    if (ec) {
      SPDLOG_WARN("Notify client termination error, ErrorCode={}, Endpoint={}", ec.message(), endpoint);
    }
  }
}

std::string clientId() {
  static std::atomic<std::uint32_t> sequence;
  return fmt::format("{}@{}#{}_{}", UtilAll::hostname(), getpid(), sequence.fetch_add(1, std::memory_order_relaxed),
                     MixAll::millisecondsOf(std::chrono::system_clock::now().time_since_epoch()));
}

ROCKETMQ_NAMESPACE_END