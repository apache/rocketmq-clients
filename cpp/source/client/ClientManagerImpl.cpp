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
#include "ClientManagerImpl.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <memory>
#include <system_error>
#include <utility>
#include <vector>

#include "InvocationContext.h"
#include "LogInterceptor.h"
#include "LogInterceptorFactory.h"
#include "MixAll.h"
#include "Protocol.h"
#include "ReceiveMessageContext.h"
#include "RpcClient.h"
#include "RpcClientImpl.h"
#include "Scheduler.h"
#include "SchedulerImpl.h"
#include "UtilAll.h"
#include "google/protobuf/util/time_util.h"
#include "grpcpp/create_channel.h"
#include "rocketmq/ErrorCode.h"
#include "spdlog/spdlog.h"

ROCKETMQ_NAMESPACE_BEGIN

ClientManagerImpl::ClientManagerImpl(std::string resource_namespace, bool with_ssl)
    : scheduler_(std::make_shared<SchedulerImpl>()),
      resource_namespace_(std::move(resource_namespace)),
      state_(State::CREATED),
      callback_thread_pool_(absl::make_unique<ThreadPoolImpl>(std::thread::hardware_concurrency())),
      with_ssl_(with_ssl) {

  certificate_verifier_ = grpc::experimental::ExternalCertificateVerifier::Create<InsecureCertificateVerifier>();
  tls_channel_credential_options_.set_verify_server_certs(false);
  tls_channel_credential_options_.set_check_call_host(false);
  channel_credential_ = grpc::experimental::TlsCredentials(tls_channel_credential_options_);

  // Use unlimited receive message size.
  channel_arguments_.SetMaxReceiveMessageSize(-1);

  int max_send_message_size = 1024 * 1024 * 16;
  channel_arguments_.SetMaxSendMessageSize(max_send_message_size);

  /*
   * Keep-alive settings:
   * https://github.com/grpc/grpc/blob/master/doc/keepalive.md
   * Keep-alive ping timeout duration: 3s
   * Keep-alive ping interval, 30s
   */
  channel_arguments_.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 60000);
  channel_arguments_.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 3000);
  channel_arguments_.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
  channel_arguments_.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);

  /*
   * If set to zero, disables retry behavior. Otherwise, transparent retries
   * are enabled for all RPCs, and configurable retries are enabled when they
   * are configured via the service config. For details, see:
   *   https://github.com/grpc/proposal/blob/master/A6-client-retries.md
   */
  channel_arguments_.SetInt(GRPC_ARG_ENABLE_RETRIES, 0);

  // channel_arguments_.SetSslTargetNameOverride("localhost");

  SPDLOG_INFO("ClientManager[ResourceNamespace={}] created", resource_namespace_);
}

ClientManagerImpl::~ClientManagerImpl() {
  shutdown();
  SPDLOG_INFO("ClientManager[ResourceNamespace={}] destructed", resource_namespace_);
}

void ClientManagerImpl::start() {
  if (State::CREATED != state_.load(std::memory_order_relaxed)) {
    SPDLOG_WARN("Unexpected client instance state: {}", state_.load(std::memory_order_relaxed));
    return;
  }

  state_.store(State::STARTING, std::memory_order_relaxed);

  callback_thread_pool_->start();
  scheduler_->start();

  std::weak_ptr<ClientManagerImpl> client_instance_weak_ptr = shared_from_this();
  auto heartbeat_functor = [client_instance_weak_ptr]() {
    auto client_instance = client_instance_weak_ptr.lock();
    if (client_instance) {
      client_instance->doHeartbeat();
    }
  };
  heartbeat_task_id_ = scheduler_->schedule(
      heartbeat_functor, HEARTBEAT_TASK_NAME, std::chrono::seconds(1), std::chrono::seconds(10));
  SPDLOG_DEBUG("Heartbeat task-id={}", heartbeat_task_id_);

  state_.store(State::STARTED, std::memory_order_relaxed);
}

void ClientManagerImpl::shutdown() {
  SPDLOG_INFO("Client manager shutdown");
  if (State::STARTED != state_.load(std::memory_order_relaxed)) {
    SPDLOG_WARN("Unexpected client instance state: {}", state_.load(std::memory_order_relaxed));
    return;
  }
  state_.store(STOPPING, std::memory_order_relaxed);

  callback_thread_pool_->shutdown();

  if (heartbeat_task_id_) {
    scheduler_->cancel(heartbeat_task_id_);
  }

  scheduler_->shutdown();

  {
    absl::MutexLock lk(&rpc_clients_mtx_);
    rpc_clients_.clear();
    SPDLOG_DEBUG("rpc_clients_ is clear");
  }

  state_.store(State::STOPPED, std::memory_order_relaxed);
  SPDLOG_DEBUG("ClientManager stopped");
}

std::vector<std::string> ClientManagerImpl::cleanOfflineRpcClients() {
  absl::flat_hash_set<std::string> hosts;
  {
    absl::MutexLock lk(&clients_mtx_);
    for (const auto& item : clients_) {
      std::shared_ptr<Client> client = item.lock();
      if (!client) {
        continue;
      }
      client->endpointsInUse(hosts);
    }
  }

  std::vector<std::string> removed;
  {
    absl::MutexLock lk(&rpc_clients_mtx_);
    for (auto it = rpc_clients_.begin(); it != rpc_clients_.end();) {
      std::string host = it->first;
      if (it->second->needHeartbeat() && !hosts.contains(host)) {
        SPDLOG_INFO("Removed RPC client whose peer is offline. RemoteHost={}", host);
        removed.push_back(host);
        rpc_clients_.erase(it++);
      } else {
        it++;
      }
    }
  }

  return removed;
}

void ClientManagerImpl::heartbeat(const std::string& target_host,
                                  const Metadata& metadata,
                                  const HeartbeatRequest& request,
                                  std::chrono::milliseconds timeout,
                                  const std::function<void(const std::error_code&, const HeartbeatResponse&)>& cb) {
  SPDLOG_DEBUG("Prepare to send heartbeat to {}. Request: {}", target_host, request.ShortDebugString());
  auto client = getRpcClient(target_host, true);
  auto invocation_context = new InvocationContext<HeartbeatResponse>();
  invocation_context->task_name = fmt::format("Heartbeat to {}", target_host);
  invocation_context->remote_address = target_host;
  for (const auto& item : metadata) {
    invocation_context->context.AddMetadata(item.first, item.second);
  }

  auto callback = [cb](const InvocationContext<HeartbeatResponse>* invocation_context) {
    if (!invocation_context->status.ok()) {
      SPDLOG_WARN("Failed to send heartbeat to target_host={}. gRPC code: {}, message: {}",
                  invocation_context->remote_address, invocation_context->status.error_code(),
                  invocation_context->status.error_message());
      std::error_code ec = ErrorCode::RequestTimeout;
      cb(ec, invocation_context->response);
      return;
    }

    auto&& status = invocation_context->response.status();
    std::error_code ec;
    switch (status.code()) {
      case rmq::Code::OK: {
        cb(ec, invocation_context->response);
        break;
      }

      case rmq::Code::ILLEGAL_CONSUMER_GROUP: {
        SPDLOG_ERROR("IllegalConsumerGroup: {}. Host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::IllegalConsumerGroup;
        break;
      }

      case rmq::Code::TOO_MANY_REQUESTS: {
        SPDLOG_WARN("TooManyRequest: {}. Host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::TooManyRequests;
        cb(ec, invocation_context->response);
        break;
      }

      case rmq::Code::UNAUTHORIZED: {
        SPDLOG_WARN("Unauthorized: {}. Host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::Unauthorized;
        cb(ec, invocation_context->response);
        break;
      }

      case rmq::Code::UNRECOGNIZED_CLIENT_TYPE: {
        SPDLOG_ERROR("UnsupportedClientType: {}. Host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::UnsupportedClientType;
        cb(ec, invocation_context->response);
        break;
      }

      case rmq::Code::CLIENT_ID_REQUIRED: {
        SPDLOG_ERROR("ClientIdRequired: {}. Host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::InternalClientError;
        cb(ec, invocation_context->response);
        break;
      }

      case rmq::Code::INTERNAL_SERVER_ERROR: {
        SPDLOG_WARN("InternalServerError: {}, host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::InternalServerError;
        cb(ec, invocation_context->response);
        break;
      }

      default: {
        SPDLOG_WARN("NotSupported: Please upgrade SDK to latest release. Message={}, host={}", status.message(),
                    invocation_context->remote_address);
        ec = ErrorCode::NotSupported;
        cb(ec, invocation_context->response);
        break;
      }
    }
  };

  invocation_context->callback = callback;
  invocation_context->context.set_deadline(std::chrono::system_clock::now() + timeout);
  client->asyncHeartbeat(request, invocation_context);
}

void ClientManagerImpl::doHeartbeat() {
  if (State::STARTED != state_.load(std::memory_order_relaxed) &&
      State::STARTING != state_.load(std::memory_order_relaxed)) {
    SPDLOG_WARN("Unexpected client manager state={}.", state_.load(std::memory_order_relaxed));
    return;
  }

  {
    absl::MutexLock lk(&clients_mtx_);
    for (const auto& item : clients_) {
      auto client = item.lock();
      if (client && client->active()) {
        client->heartbeat();
      }
    }
  }
}

bool ClientManagerImpl::send(const std::string& target_host,
                             const Metadata& metadata,
                             SendMessageRequest& request,
                             SendResultCallback cb) {
  assert(cb);
  SPDLOG_DEBUG("Prepare to send message to {} asynchronously. Request: {}", target_host, request.ShortDebugString());
  RpcClientSharedPtr client = getRpcClient(target_host);
  // Invocation context will be deleted in its onComplete() method.
  auto invocation_context = new InvocationContext<SendMessageResponse>();
  invocation_context->task_name = fmt::format("Send message to {}", target_host);
  invocation_context->remote_address = target_host;
  for (const auto& entry : metadata) {
    invocation_context->context.AddMetadata(entry.first, entry.second);
  }

  const std::string& topic = request.messages().begin()->topic().name();
  std::weak_ptr<ClientManager> client_manager(shared_from_this());
  auto completion_callback = [topic, cb, client_manager,
                              target_host](const InvocationContext<SendMessageResponse>* invocation_context) {
    ClientManagerPtr client_manager_ptr = client_manager.lock();
    if (!client_manager_ptr) {
      return;
    }

    if (State::STARTED != client_manager_ptr->state()) {
      // TODO: Would this leak some memory?
      return;
    }

    SendResult send_result = {};
    send_result.target = target_host;
    if (!invocation_context->status.ok()) {
      SPDLOG_WARN("Failed to send message to {} due to gRPC error. gRPC code: {}, gRPC error message: {}",
                  invocation_context->remote_address, invocation_context->status.error_code(),
                  invocation_context->status.error_message());
      send_result.ec = ErrorCode::RequestTimeout;
      cb(send_result);
      return;
    }

    auto&& status = invocation_context->response.status();
    switch (invocation_context->response.status().code()) {
      case rmq::Code::OK: {
        if (!invocation_context->response.entries().empty()) {
          auto first = invocation_context->response.entries().begin();
          send_result.message_id = first->message_id();
          send_result.transaction_id = first->transaction_id();
          // unique handle to identify a message to recall,
          // only delay message is supported for now
          send_result.recall_handle = first->recall_handle();
        } else {
          SPDLOG_ERROR("Unexpected send-message-response: {}", invocation_context->response.ShortDebugString());
        }
        break;
      }

      case rmq::Code::ILLEGAL_TOPIC: {
        SPDLOG_ERROR("IllegalTopic: {}. Host={}", status.message(), invocation_context->remote_address);
        send_result.ec = ErrorCode::IllegalTopic;
        break;
      }

      case rmq::Code::ILLEGAL_MESSAGE_TAG: {
        SPDLOG_ERROR("IllegalMessageTag: {}. Host={}", status.message(), invocation_context->remote_address);
        send_result.ec = ErrorCode::IllegalMessageTag;
        break;
      }

      case rmq::Code::ILLEGAL_MESSAGE_KEY: {
        SPDLOG_ERROR("IllegalMessageKey: {}. Host={}", status.message(), invocation_context->remote_address);
        send_result.ec = ErrorCode::IllegalMessageKey;
        break;
      }

      case rmq::Code::ILLEGAL_MESSAGE_GROUP: {
        SPDLOG_ERROR("IllegalMessageGroup: {}. Host={}", status.message(), invocation_context->remote_address);
        send_result.ec = ErrorCode::IllegalMessageGroup;
        break;
      }

      case rmq::Code::ILLEGAL_MESSAGE_PROPERTY_KEY: {
        SPDLOG_ERROR("IllegalMessageProperty: {}. Host={}", status.message(), invocation_context->remote_address);
        send_result.ec = ErrorCode::IllegalMessageProperty;
        break;
      }

      case rmq::Code::ILLEGAL_DELIVERY_TIME: {
        SPDLOG_ERROR("IllegalDeliveryTime: {}. Host={}", status.message(), invocation_context->remote_address);
        send_result.ec = ErrorCode::IllegalMessageProperty;
        break;
      }

      case rmq::Code::MESSAGE_PROPERTIES_TOO_LARGE: {
        SPDLOG_ERROR("MessagePropertiesTooLarge: {}. Host={}", status.message(), invocation_context->remote_address);
        send_result.ec = ErrorCode::MessagePropertiesTooLarge;
        break;
      }

      case rmq::Code::MESSAGE_BODY_TOO_LARGE: {
        SPDLOG_ERROR("MessageBodyTooLarge: {}. Host={}", status.message(), invocation_context->remote_address);
        send_result.ec = ErrorCode::MessageBodyTooLarge;
        break;
      }

      case rmq::Code::MESSAGE_BODY_EMPTY: {
        SPDLOG_ERROR("MessageBodyEmpty: {}. Host={}", status.message(), invocation_context->remote_address);
        send_result.ec = ErrorCode::MessageBodyTooLarge;
        break;
      }

      case rmq::Code::TOPIC_NOT_FOUND: {
        SPDLOG_WARN("TopicNotFound: {}. Host={}", status.message(), invocation_context->remote_address);
        send_result.ec = ErrorCode::TopicNotFound;
        break;
      }

      case rmq::Code::NOT_FOUND: {
        SPDLOG_WARN("NotFound: {}. Host={}", status.message(), invocation_context->remote_address);
        send_result.ec = ErrorCode::NotFound;
        break;
      }

      case rmq::Code::UNAUTHORIZED: {
        SPDLOG_WARN("Unauthenticated: {}. Host={}", status.message(), invocation_context->remote_address);
        send_result.ec = ErrorCode::Unauthorized;
        break;
      }

      case rmq::Code::FORBIDDEN: {
        SPDLOG_WARN("Forbidden: {}. Host={}", status.message(), invocation_context->remote_address);
        send_result.ec = ErrorCode::Forbidden;
        break;
      }

      case rmq::Code::MESSAGE_CORRUPTED: {
        SPDLOG_WARN("MessageCorrupted: {}. Host={}", status.message(), invocation_context->remote_address);
        send_result.ec = ErrorCode::MessageCorrupted;
        break;
      }

      case rmq::Code::TOO_MANY_REQUESTS: {
        SPDLOG_WARN("TooManyRequest: {}. Host={}", status.message(), invocation_context->remote_address);
        send_result.ec = ErrorCode::TooManyRequests;
        break;
      }

      case rmq::Code::INTERNAL_SERVER_ERROR: {
        SPDLOG_WARN("InternalServerError: {}. Host={}", status.message(), invocation_context->remote_address);
        send_result.ec = ErrorCode::InternalServerError;
        break;
      }

      case rmq::Code::HA_NOT_AVAILABLE: {
        SPDLOG_WARN("InternalServerError: {}. Host={}", status.message(), invocation_context->remote_address);
        send_result.ec = ErrorCode::InternalServerError;
        break;
      }

      case rmq::Code::PROXY_TIMEOUT: {
        SPDLOG_WARN("GatewayTimeout: {}. Host={}", status.message(), invocation_context->remote_address);
        send_result.ec = ErrorCode::GatewayTimeout;
        break;
      }

      case rmq::Code::MASTER_PERSISTENCE_TIMEOUT: {
        SPDLOG_WARN("GatewayTimeout: {}. Host={}", status.message(), invocation_context->remote_address);
        send_result.ec = ErrorCode::GatewayTimeout;
        break;
      }

      case rmq::Code::SLAVE_PERSISTENCE_TIMEOUT: {
        SPDLOG_WARN("GatewayTimeout: {}. Host={}", status.message(), invocation_context->remote_address);
        send_result.ec = ErrorCode::GatewayTimeout;
        break;
      }

      case rmq::Code::MESSAGE_PROPERTY_CONFLICT_WITH_TYPE: {
        SPDLOG_WARN("Message-property-conflict-with-type: Host={}, Response={}", invocation_context->remote_address,
                    invocation_context->response.ShortDebugString());
        send_result.ec = ErrorCode::MessagePropertyConflictWithType;
        break;
      }

      default: {
        SPDLOG_WARN("NotSupported: Check and upgrade SDK to the latest. Host={}", invocation_context->remote_address);
        send_result.ec = ErrorCode::NotSupported;
        break;
      }
    }

    cb(send_result);
  };

  invocation_context->callback = completion_callback;
  client->asyncSend(request, invocation_context);
  return true;
}

/**
 * @brief Create a gRPC channel to target host.
 *
 * @param target_host
 * @return std::shared_ptr<grpc::Channel>
 */
std::shared_ptr<grpc::Channel> ClientManagerImpl::createChannel(const std::string& target_host) {
  std::vector<std::unique_ptr<grpc::experimental::ClientInterceptorFactoryInterface>> interceptor_factories;
  interceptor_factories.emplace_back(absl::make_unique<LogInterceptorFactory>());
  auto channel = grpc::experimental::CreateCustomChannelWithInterceptors(
      target_host, with_ssl_ ? channel_credential_ : grpc::InsecureChannelCredentials(), channel_arguments_,
      std::move(interceptor_factories));
  return channel;
}

RpcClientSharedPtr ClientManagerImpl::getRpcClient(const std::string& target_host, bool need_heartbeat) {
  std::shared_ptr<RpcClient> client;
  {
    absl::MutexLock lock(&rpc_clients_mtx_);
    auto search = rpc_clients_.find(target_host);
    if (search == rpc_clients_.end() || !search->second->ok()) {
      if (search == rpc_clients_.end()) {
        SPDLOG_INFO("Create a RPC client to [{}]", target_host.data());
      } else if (!search->second->ok()) {
        SPDLOG_INFO("Prior RPC client to {} is not OK. Re-create one", target_host);
      }
      auto channel = createChannel(target_host);
      std::weak_ptr<ClientManager> client_manager(shared_from_this());
      client = std::make_shared<RpcClientImpl>(client_manager, channel, target_host, need_heartbeat);
      rpc_clients_.insert_or_assign(target_host, client);
    } else {
      client = search->second;
    }
  }

  if (need_heartbeat && !client->needHeartbeat()) {
    client->needHeartbeat(need_heartbeat);
  }

  return client;
}

void ClientManagerImpl::addRpcClient(const std::string& target_host, const RpcClientSharedPtr& client) {
  {
    absl::MutexLock lock(&rpc_clients_mtx_);
    rpc_clients_.insert_or_assign(target_host, client);
  }
}

void ClientManagerImpl::cleanRpcClients() {
  absl::MutexLock lk(&rpc_clients_mtx_);
  rpc_clients_.clear();
}

SendResult ClientManagerImpl::processSendResponse(const rmq::MessageQueue& message_queue,
                                                  const SendMessageResponse& response,
                                                  std::error_code& ec) {
  SendResult send_result;

  switch (response.status().code()) {
    case rmq::Code::OK: {
      assert(response.entries_size() > 0);
      send_result.message_id = response.entries().begin()->message_id();
      send_result.transaction_id = response.entries().begin()->transaction_id();
      return send_result;
    }
    case rmq::Code::ILLEGAL_TOPIC: {
      ec = ErrorCode::BadRequest;
      return send_result;
    }
    default: {
      // TODO: handle other cases.
      break;
    }
  }
  return send_result;
}

void ClientManagerImpl::addClientObserver(std::weak_ptr<Client> client) {
  absl::MutexLock lk(&clients_mtx_);
  clients_.emplace_back(std::move(client));
}

void ClientManagerImpl::resolveRoute(const std::string& target_host,
                                     const Metadata& metadata,
                                     const QueryRouteRequest& request,
                                     std::chrono::milliseconds timeout,
                                     const std::function<void(const std::error_code&, const TopicRouteDataPtr&)>& cb) {
  SPDLOG_DEBUG("Name server connection URL: {}", target_host);
  SPDLOG_DEBUG("Query route request: {}", request.ShortDebugString());
  RpcClientSharedPtr client = getRpcClient(target_host, false);
  if (!client) {
    SPDLOG_WARN("Failed to create RPC client for name server[host={}]", target_host);
    std::error_code ec = ErrorCode::RequestTimeout;
    cb(ec, nullptr);
    return;
  }

  auto invocation_context = new InvocationContext<QueryRouteResponse>();
  invocation_context->task_name = fmt::format("Query route of topic={} from {}", request.topic().name(), target_host);
  invocation_context->remote_address = target_host;
  invocation_context->context.set_deadline(std::chrono::system_clock::now() + timeout);
  for (const auto& item : metadata) {
    invocation_context->context.AddMetadata(item.first, item.second);
  }

  auto callback = [cb](const InvocationContext<QueryRouteResponse>* invocation_context) {
    if (!invocation_context->status.ok()) {
      SPDLOG_WARN("Failed to send query route request to server[host={}]. Reason: {}",
                  invocation_context->remote_address, invocation_context->status.error_message());
      std::error_code ec = ErrorCode::RequestTimeout;
      cb(ec, nullptr);
      return;
    }

    std::error_code ec;
    auto&& status = invocation_context->response.status();
    switch (status.code()) {
      case rmq::Code::OK: {
        std::vector<rmq::MessageQueue> message_queues;
        for (const auto& item : invocation_context->response.message_queues()) {
          message_queues.push_back(item);
        }
        auto ptr = std::make_shared<TopicRouteData>(std::move(message_queues));
        cb(ec, ptr);
        break;
      }

      case rmq::Code::ILLEGAL_ACCESS_POINT: {
        SPDLOG_WARN("IllegalAccessPoint: {}. Host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::IllegalAccessPoint;
        cb(ec, nullptr);
        break;
      }

      case rmq::Code::UNAUTHORIZED: {
        SPDLOG_WARN("Unauthorized: {}. Host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::Unauthorized;
        cb(ec, nullptr);
        break;
      }

      case rmq::Code::TOPIC_NOT_FOUND: {
        SPDLOG_WARN("TopicNotFound: {}. Host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::NotFound;
        cb(ec, nullptr);
        break;
      }

      case rmq::Code::TOO_MANY_REQUESTS: {
        SPDLOG_WARN("TooManyRequest: {}. Host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::TooManyRequests;
        cb(ec, nullptr);
        break;
      }

      case rmq::Code::CLIENT_ID_REQUIRED: {
        SPDLOG_ERROR("ClientIdRequired: {}. Host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::InternalClientError;
        cb(ec, nullptr);
        break;
      }

      case rmq::Code::INTERNAL_SERVER_ERROR: {
        SPDLOG_WARN("InternalServerError: {}. Host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::InternalServerError;
        cb(ec, nullptr);
        break;
      }

      case rmq::Code::PROXY_TIMEOUT: {
        SPDLOG_WARN("GatewayTimeout: {}. Host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::GatewayTimeout;
        cb(ec, nullptr);
        break;
      }

      default: {
        SPDLOG_WARN("NotImplement: Please upgrade to latest SDK release. Host={}", invocation_context->remote_address);
        ec = ErrorCode::NotImplemented;
        cb(ec, nullptr);
        break;
      }
    }
  };
  invocation_context->callback = callback;
  client->asyncQueryRoute(request, invocation_context);
}

void ClientManagerImpl::queryAssignment(
    const std::string& target,
    const Metadata& metadata,
    const QueryAssignmentRequest& request,
    std::chrono::milliseconds timeout,
    const std::function<void(const std::error_code&, const QueryAssignmentResponse&)>& cb) {
  SPDLOG_DEBUG("Prepare to send query assignment request to broker[address={}]", target);
  std::shared_ptr<RpcClient> client = getRpcClient(target);

  auto callback = [&, cb](const InvocationContext<QueryAssignmentResponse>* invocation_context) {
    if (!invocation_context->status.ok()) {
      SPDLOG_WARN("Failed to query assignment. Reason: {}", invocation_context->status.error_message());
      std::error_code ec = ErrorCode::RequestTimeout;
      cb(ec, invocation_context->response);
      return;
    }

    auto&& status = invocation_context->response.status();
    std::error_code ec;
    switch (status.code()) {
      case rmq::Code::OK: {
        SPDLOG_DEBUG("Query assignment OK. Host={}", invocation_context->remote_address);
        break;
      }

      case rmq::Code::ILLEGAL_ACCESS_POINT: {
        SPDLOG_WARN("IllegalAccessPoint: {}, host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::IllegalAccessPoint;
        break;
      }

      case rmq::Code::ILLEGAL_TOPIC: {
        SPDLOG_WARN("IllegalAccessPoint: {}. Host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::IllegalTopic;
        break;
      }

      case rmq::Code::ILLEGAL_CONSUMER_GROUP: {
        SPDLOG_WARN("IllegalConsumerGroup: {}. Host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::IllegalConsumerGroup;
        break;
      }

      case rmq::Code::CLIENT_ID_REQUIRED: {
        SPDLOG_WARN("ClientIdRequired: {}. Host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::InternalClientError;
        break;
      }

      case rmq::Code::UNAUTHORIZED: {
        SPDLOG_WARN("Unauthorized: {}, host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::Unauthorized;
        break;
      }

      case rmq::Code::FORBIDDEN: {
        SPDLOG_WARN("Forbidden: {}, host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::Forbidden;
        break;
      }

      case rmq::Code::TOPIC_NOT_FOUND: {
        SPDLOG_WARN("TopicNotFound: {}, host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::TopicNotFound;
        break;
      }

      case rmq::Code::CONSUMER_GROUP_NOT_FOUND: {
        SPDLOG_WARN("ConsumerGroupNotFound: {}, host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::ConsumerGroupNotFound;
        break;
      }

      case rmq::Code::INTERNAL_SERVER_ERROR: {
        SPDLOG_WARN("InternalServerError: {}, host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::InternalServerError;
        break;
      }

      case rmq::Code::PROXY_TIMEOUT: {
        SPDLOG_WARN("GatewayTimeout: {}. Host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::GatewayTimeout;
        break;
      }

      default: {
        SPDLOG_WARN("NotSupported: please upgrade SDK to latest release. Host={}", invocation_context->remote_address);
        ec = ErrorCode::NotSupported;
        break;
      }
    }
    cb(ec, invocation_context->response);
  };

  auto invocation_context = new InvocationContext<QueryAssignmentResponse>();
  invocation_context->task_name = fmt::format("QueryAssignment from {}", target);
  invocation_context->remote_address = target;
  for (const auto& item : metadata) {
    invocation_context->context.AddMetadata(item.first, item.second);
  }
  invocation_context->context.set_deadline(std::chrono::system_clock::now() + timeout);
  invocation_context->callback = callback;
  client->asyncQueryAssignment(request, invocation_context);
}

void ClientManagerImpl::receiveMessage(const std::string& target_host,
                                       const Metadata& metadata,
                                       const ReceiveMessageRequest& request,
                                       std::chrono::milliseconds timeout,
                                       ReceiveMessageCallback cb) {
  SPDLOG_DEBUG("Prepare to receive message from {} asynchronously. Request: {}", target_host, request.ShortDebugString());
  RpcClientSharedPtr client = getRpcClient(target_host);
  auto context = absl::make_unique<ReceiveMessageContext>();
  context->callback = std::move(cb);
  context->metadata = metadata;
  context->timeout = timeout;
  client->asyncReceive(request, std::move(context));
}

State ClientManagerImpl::state() const {
  return state_.load(std::memory_order_relaxed);
}

MessageConstSharedPtr ClientManagerImpl::wrapMessage(const rmq::Message& item) {
  auto builder = Message::newBuilder();

  // base
  builder.withTopic(item.topic().name());

  const auto& system_properties = item.system_properties();

  // Tag
  if (system_properties.has_tag()) {
    builder.withTag(system_properties.tag());
  }

  // Keys
  std::vector<std::string> keys;
  for (const auto& key : system_properties.keys()) {
    keys.push_back(key);
  }
  if (!keys.empty()) {
    builder.withKeys(std::move(keys));
  }

  // Message Group
  if (system_properties.has_message_group()) {
    builder.withGroup(system_properties.message_group());
  }

  // Message-Id
  const auto& message_id = system_properties.message_id();
  builder.withId(message_id);

  // Validate body digest
  const rmq::Digest& digest = system_properties.body_digest();
  bool body_digest_match = false;
  if (item.body().empty()) {
    SPDLOG_WARN("Body of message[topic={}, msgId={}] is empty", item.topic().name(),
                item.system_properties().message_id());
    body_digest_match = true;
  } else {
    switch (digest.type()) {
      case rmq::DigestType::CRC32: {
        std::string checksum;
        bool success = MixAll::crc32(item.body(), checksum);
        if (success) {
          body_digest_match = (digest.checksum() == checksum);
          if (body_digest_match) {
            SPDLOG_DEBUG("Message body CRC32 checksum validation passed.");
          } else {
            SPDLOG_WARN("Body CRC32 checksum validation failed. Actual: {}, expect: {}", checksum, digest.checksum());
          }
        } else {
          SPDLOG_WARN("Failed to calculate CRC32 checksum. Skip.");
        }
        break;
      }
      case rmq::DigestType::MD5: {
        std::string checksum;
        bool success = MixAll::md5(item.body(), checksum);
        if (success) {
          body_digest_match = (digest.checksum() == checksum);
          if (body_digest_match) {
            SPDLOG_DEBUG("Body of message[{}] MD5 checksum validation passed.", message_id);
          } else {
            SPDLOG_WARN("Body of message[{}] MD5 checksum validation failed. Expect: {}, Actual: {}", message_id,
                        digest.checksum(), checksum);
          }
        } else {
          SPDLOG_WARN("Failed to calculate MD5 digest. Skip.");
          body_digest_match = true;
        }
        break;
      }
      case rmq::DigestType::SHA1: {
        std::string checksum;
        bool success = MixAll::sha1(item.body(), checksum);
        if (success) {
          body_digest_match = (checksum == digest.checksum());
          if (body_digest_match) {
            SPDLOG_DEBUG("Body of message[{}] SHA1 checksum validation passed", message_id);
          } else {
            SPDLOG_WARN("Body of message[{}] SHA1 checksum validation failed. Expect: {}, Actual: {}", message_id,
                        digest.checksum(), checksum);
          }
        } else {
          SPDLOG_WARN("Failed to calculate SHA1 digest for message[{}]. Skip.", message_id);
        }
        break;
      }
      default: {
        SPDLOG_WARN("Unsupported message body digest algorithm");
        body_digest_match = true;
        break;
      }
    }
  }

  if (!body_digest_match) {
    SPDLOG_WARN("Message body checksum failed. MsgId={}", system_properties.message_id());
    // TODO: NACK it immediately
    return nullptr;
  }

  // Body encoding
  switch (system_properties.body_encoding()) {
    case rmq::Encoding::GZIP: {
      std::string uncompressed;
      UtilAll::uncompress(item.body(), uncompressed);
      builder.withBody(uncompressed);
      break;
    }
    case rmq::Encoding::IDENTITY: {
      builder.withBody(item.body());
      break;
    }
    default: {
      SPDLOG_WARN("Unsupported encoding algorithm");
      break;
    }
  }

  // User-properties
  std::unordered_map<std::string, std::string> properties;
  for (const auto& it : item.user_properties()) {
    properties.insert(std::make_pair(it.first, it.second));
  }
  if (!properties.empty()) {
    builder.withProperties(properties);
  }

  // Born-timestamp
  if (system_properties.has_born_timestamp()) {
    auto born_timestamp = google::protobuf::util::TimeUtil::TimestampToMilliseconds(system_properties.born_timestamp());
    builder.withBornTime(absl::ToChronoTime(absl::FromUnixMillis(born_timestamp)));
  }

  // Born-host
  builder.withBornHost(system_properties.born_host());

  // Trace-context
  if (system_properties.has_trace_context()) {
    builder.withTraceContext(system_properties.trace_context());
  }

  auto message = builder.build();

  const Message* raw = message.release();
  Message* msg = const_cast<Message*>(raw);
  Extension& extension = msg->mutableExtension();

  // Receipt-handle
  extension.receipt_handle = system_properties.receipt_handle();

  // Store-timestamp
  if (system_properties.has_store_timestamp()) {
    auto store_timestamp =
        google::protobuf::util::TimeUtil::TimestampToMilliseconds(system_properties.store_timestamp());
    extension.store_time = absl::ToChronoTime(absl::FromUnixMillis(store_timestamp));
  }

  // Store-host
  extension.store_host = system_properties.store_host();

  // Process one-of: delivery-timestamp and delay-level.
  if (system_properties.has_delivery_timestamp()) {
    auto delivery_timestamp_ms =
        google::protobuf::util::TimeUtil::TimestampToMilliseconds(system_properties.delivery_timestamp());
    extension.delivery_timepoint = absl::ToChronoTime(absl::FromUnixMillis(delivery_timestamp_ms));
  }

  // Queue-id
  extension.queue_id = system_properties.queue_id();

  // Queue-offset
  extension.offset = system_properties.queue_offset();

  // Invisible-period
  if (system_properties.has_invisible_duration()) {
    auto invisible_period = std::chrono::seconds(system_properties.invisible_duration().seconds()) +
                            std::chrono::nanoseconds(system_properties.invisible_duration().nanos());
    extension.invisible_period = invisible_period;
  }

  // Delivery attempt
  extension.delivery_attempt = system_properties.delivery_attempt();

  // Decoded Time-Point
  extension.decode_time = std::chrono::system_clock::now();

  return MessageConstSharedPtr(raw);
}

SchedulerSharedPtr ClientManagerImpl::getScheduler() {
  return scheduler_;
}

void ClientManagerImpl::ack(const std::string& target,
                            const Metadata& metadata,
                            const AckMessageRequest& request,
                            std::chrono::milliseconds timeout,
                            const std::function<void(const std::error_code&)>& cb) {
  std::string target_host(target.data(), target.length());
  SPDLOG_DEBUG("Prepare to ack message against {} asynchronously. AckMessageRequest: {}", target_host,
               request.DebugString());
  RpcClientSharedPtr client = getRpcClient(target_host);

  auto invocation_context = new InvocationContext<AckMessageResponse>();
  invocation_context->task_name = fmt::format("Ack messages against {}", target);
  invocation_context->remote_address = target_host;
  invocation_context->context.set_deadline(std::chrono::system_clock::now() + timeout);

  for (const auto& item : metadata) {
    invocation_context->context.AddMetadata(item.first, item.second);
  }

  // TODO: Use capture by move and pass-by-value paradigm when C++ 14 is available.
  auto callback = [request, cb](const InvocationContext<AckMessageResponse>* invocation_context) {
    std::error_code ec;
    if (!invocation_context->status.ok()) {
      ec = ErrorCode::RequestTimeout;
      cb(ec);
      return;
    }

    auto&& status = invocation_context->response.status();
    switch (status.code()) {
      case rmq::Code::OK: {
        SPDLOG_DEBUG("Ack OK. host={}", invocation_context->remote_address);
        break;
      }

      case rmq::Code::MULTIPLE_RESULTS: {
        SPDLOG_DEBUG("Server returns multiple results. host={}", invocation_context->remote_address);
        // Treat it as successful, allowing top tier processing according to response entries.
        break;
      }

      case rmq::Code::ILLEGAL_TOPIC: {
        SPDLOG_WARN("IllegalTopic: {}, host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::IllegalTopic;
        break;
      }

      case rmq::Code::ILLEGAL_CONSUMER_GROUP: {
        SPDLOG_WARN("IllegalConsumerGroup: {}, host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::IllegalConsumerGroup;
        break;
      }

      case rmq::Code::INVALID_RECEIPT_HANDLE: {
        SPDLOG_WARN("InvalidReceiptHandle: {}, host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::InvalidReceiptHandle;
        break;
      }

      case rmq::Code::CLIENT_ID_REQUIRED: {
        SPDLOG_WARN("ClientIdRequired: {}, host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::InternalClientError;
        break;
      }

      case rmq::Code::TOPIC_NOT_FOUND: {
        SPDLOG_WARN("TopicNotFound: {}, host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::TopicNotFound;
        break;
      }

      case rmq::Code::UNAUTHORIZED: {
        SPDLOG_WARN("Unauthorized: {}, host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::Unauthorized;
        break;
      }

      case rmq::Code::FORBIDDEN: {
        SPDLOG_WARN("PermissionDenied: {}, host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::Forbidden;
        break;
      }

      case rmq::Code::TOO_MANY_REQUESTS: {
        SPDLOG_WARN("TooManyRequests: {}, host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::TooManyRequests;
        break;
      }

      case rmq::Code::INTERNAL_SERVER_ERROR: {
        SPDLOG_WARN("InternalServerError: {}, host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::InternalServerError;
        break;
      }

      case rmq::Code::PROXY_TIMEOUT: {
        SPDLOG_WARN("GatewayTimeout: {}, host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::GatewayTimeout;
        break;
      }

      default: {
        SPDLOG_WARN("NotSupported: please upgrade SDK to latest release. host={}", invocation_context->remote_address);
        ec = ErrorCode::NotSupported;
        break;
      }
    }
    cb(ec);
  };
  invocation_context->callback = callback;
  client->asyncAck(request, invocation_context);
}

void ClientManagerImpl::changeInvisibleDuration(
    const std::string& target_host,
    const Metadata& metadata,
    const ChangeInvisibleDurationRequest& request,
    std::chrono::milliseconds timeout,
    const std::function<void(const std::error_code&, const ChangeInvisibleDurationResponse&)>& completion_callback) {

  RpcClientSharedPtr client = getRpcClient(target_host);
  assert(client);
  auto invocation_context = new InvocationContext<ChangeInvisibleDurationResponse>();
  invocation_context->task_name = fmt::format("ChangeInvisibleDuration Message[receipt-handle={}] against {}",
                                              request.receipt_handle(), target_host);
  invocation_context->remote_address = target_host;
  invocation_context->context.set_deadline(std::chrono::system_clock::now() + timeout);

  for (const auto& item : metadata) {
    invocation_context->context.AddMetadata(item.first, item.second);
  }

  auto callback = [completion_callback](const InvocationContext<ChangeInvisibleDurationResponse>* invocation_context) {
    if (!invocation_context->status.ok()) {
      SPDLOG_WARN("Failed to write Nack request to wire. gRPC-code: {}, gRPC-message: {}",
                  invocation_context->status.error_code(), invocation_context->status.error_message());
      std::error_code ec = ErrorCode::RequestTimeout;
      completion_callback(ec, invocation_context->response);
      return;
    }

    std::error_code ec;
    auto&& status = invocation_context->response.status();
    auto&& peer_address = invocation_context->remote_address;
    switch (status.code()) {
      case rmq::Code::OK: {
        SPDLOG_DEBUG("ChangeInvisibleDuration to {} OK", peer_address);
        break;
      };

      case rmq::Code::ILLEGAL_TOPIC: {
        SPDLOG_WARN("IllegalTopic: {}. Host={}", status.message(), peer_address);
        ec = ErrorCode::IllegalTopic;
        break;
      }

      case rmq::Code::ILLEGAL_CONSUMER_GROUP: {
        SPDLOG_WARN("IllegalConsumerGroup: {}. Host={}", status.message(), peer_address);
        ec = ErrorCode::IllegalConsumerGroup;
        break;
      }

      case rmq::Code::UNAUTHORIZED: {
        SPDLOG_WARN("Unauthorized: {}, host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::Unauthorized;
        break;
      }

      case rmq::Code::INVALID_RECEIPT_HANDLE: {
        SPDLOG_WARN("InvalidReceiptHandle: {}, host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::InvalidReceiptHandle;
        break;
      }

      case rmq::Code::CLIENT_ID_REQUIRED: {
        SPDLOG_WARN("ClientIdRequired: {}, host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::InternalClientError;
        break;
      }

      case rmq::Code::FORBIDDEN: {
        SPDLOG_WARN("Forbidden: {}, host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::Forbidden;
        break;
      }

      case rmq::Code::INTERNAL_SERVER_ERROR: {
        SPDLOG_WARN("InternalServerError: {}, host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::InternalServerError;
        break;
      }

      case rmq::Code::TOO_MANY_REQUESTS: {
        SPDLOG_WARN("TooManyRequests: {}, host={}", status.message(), invocation_context->remote_address);
        ec = ErrorCode::TooManyRequests;
        break;
      }

      default: {
        SPDLOG_WARN("NotSupported: Please upgrade to latest SDK, host={}", invocation_context->remote_address);
        ec = ErrorCode::NotSupported;
        break;
      }
    }
    completion_callback(ec, invocation_context->response);
  };
  invocation_context->callback = callback;
  client->asyncChangeInvisibleDuration(request, invocation_context);
}

void ClientManagerImpl::endTransaction(
    const std::string& target_host,
    const Metadata& metadata,
    const EndTransactionRequest& request,
    std::chrono::milliseconds timeout,
    const std::function<void(const std::error_code&, const EndTransactionResponse&)>& cb) {
  RpcClientSharedPtr client = getRpcClient(target_host);
  if (!client) {
    SPDLOG_WARN("No RPC client for {}", target_host);
    EndTransactionResponse response;
    std::error_code ec = ErrorCode::BadRequest;
    cb(ec, response);
    return;
  }

  SPDLOG_DEBUG("Prepare to endTransaction. TargetHost={}, Request: {}", target_host.data(), request.DebugString());

  auto invocation_context = new InvocationContext<EndTransactionResponse>();
  invocation_context->task_name = fmt::format("End transaction[{}] of message[] against {}", request.transaction_id(),
                                              request.message_id(), target_host);
  invocation_context->remote_address = target_host;
  for (const auto& item : metadata) {
    invocation_context->context.AddMetadata(item.first, item.second);
  }

  // Set RPC deadline.
  auto deadline = std::chrono::system_clock::now() + timeout;
  invocation_context->context.set_deadline(deadline);

  auto callback = [target_host, cb](const InvocationContext<EndTransactionResponse>* invocation_context) {
    std::error_code ec;
    if (!invocation_context->status.ok()) {
      SPDLOG_WARN("Failed to write EndTransaction to wire. gRPC-code: {}, gRPC-message: {}, host={}",
                  invocation_context->status.error_code(), invocation_context->status.error_message(),
                  invocation_context->remote_address);
      ec = ErrorCode::BadRequest;
      cb(ec, invocation_context->response);
      return;
    }

    auto&& status = invocation_context->response.status();
    auto&& peer_address = invocation_context->remote_address;
    switch (status.code()) {
      case rmq::Code::OK: {
        SPDLOG_DEBUG("endTransaction completed OK. Response: {}, host={}", invocation_context->response.DebugString(),
                     peer_address);
        break;
      }

      case rmq::Code::ILLEGAL_TOPIC: {
        SPDLOG_WARN("IllegalTopic: {}, host={}", status.message(), peer_address);
        ec = ErrorCode::IllegalTopic;
        break;
      }

      case rmq::Code::ILLEGAL_CONSUMER_GROUP: {
        SPDLOG_WARN("IllegalConsumerGroup: {}, host={}", status.message(), peer_address);
        ec = ErrorCode::IllegalConsumerGroup;
        break;
      }

      case rmq::Code::INVALID_TRANSACTION_ID: {
        SPDLOG_WARN("InvalidTransactionId: {}, host={}", status.message(), peer_address);
        ec = ErrorCode::InvalidTransactionId;
        break;
      }

      case rmq::Code::CLIENT_ID_REQUIRED: {
        SPDLOG_WARN("ClientIdRequired: {}, host={}", status.message(), peer_address);
        ec = ErrorCode::InternalClientError;
        break;
      }

      case rmq::Code::TOPIC_NOT_FOUND: {
        SPDLOG_WARN("TopicNotFound: {}, host={}", status.message(), peer_address);
        ec = ErrorCode::TopicNotFound;
        break;
      }

      case rmq::Code::UNAUTHORIZED: {
        SPDLOG_WARN("Unauthorized: {}, host={}", status.message(), peer_address);
        ec = ErrorCode::Unauthorized;
        break;
      }

      case rmq::Code::FORBIDDEN: {
        SPDLOG_WARN("Forbidden: {}, host={}", status.message(), peer_address);
        ec = ErrorCode::Forbidden;
        break;
      }

      case rmq::Code::INTERNAL_SERVER_ERROR: {
        SPDLOG_WARN("InternalServerError: {}, host={}", status.message(), peer_address);
        ec = ErrorCode::InternalServerError;
        break;
      }

      case rmq::Code::PROXY_TIMEOUT: {
        SPDLOG_WARN("GatewayTimeout: {}, host={}", status.message(), peer_address);
        ec = ErrorCode::GatewayTimeout;
        break;
      }

      default: {
        SPDLOG_WARN("NotSupported: please upgrade SDK to latest release. {}, host={}", status.message(), peer_address);
        ec = ErrorCode::NotSupported;
        break;
      }
    }
    cb(ec, invocation_context->response);
  };

  invocation_context->callback = callback;
  client->asyncEndTransaction(request, invocation_context);
}

void ClientManagerImpl::recallMessage(const std::string& target_host, const Metadata& metadata,
                                      const RecallMessageRequest& request, std::chrono::milliseconds timeout,
                                      const std::function<void(const std::error_code&, const RecallMessageResponse&)>& cb) {

  SPDLOG_DEBUG("RecallMessage Request: {}", request.ShortDebugString());

  RpcClientSharedPtr client = getRpcClient(target_host);
  if (!client) {
    SPDLOG_WARN("No RPC client for {}", target_host);
    RecallMessageResponse response;
    std::error_code ec = ErrorCode::BadRequest;
    cb(ec, response);
    return;
  }

  auto invocation_context = new InvocationContext<RecallMessageResponse>();
  invocation_context->task_name = fmt::format("Recall message, topic={}, recall handle={} to {}",
                                              request.topic().ShortDebugString(), request.recall_handle().data(), target_host);
  invocation_context->remote_address = target_host;
  for (const auto& item : metadata) {
    invocation_context->context.AddMetadata(item.first, item.second);
  }
  invocation_context->context.set_deadline(std::chrono::system_clock::now() + timeout);

  auto callback =
      [target_host, cb](const InvocationContext<RecallMessageResponse>* invocation_context) {

    std::error_code ec;
    if (!invocation_context->status.ok()) {
      SPDLOG_WARN("Failed to write EndTransaction to wire. gRPC-code: {}, gRPC-message: {}, host={}",
                  invocation_context->status.error_code(), invocation_context->status.error_message(),
                  invocation_context->remote_address);
      ec = ErrorCode::BadRequest;
      cb(ec, invocation_context->response);
      return;
    }

    auto&& status = invocation_context->response.status();
    auto&& peer_address = invocation_context->remote_address;
    switch (status.code()) {
      case rmq::Code::OK: {
        SPDLOG_DEBUG("Recall message OK. Response: {}, host={}",
                     invocation_context->response.ShortDebugString(), peer_address);
        break;
      }

      case rmq::Code::ILLEGAL_TOPIC: {
        SPDLOG_WARN("IllegalTopic: {}, host={}", status.message(), peer_address);
        ec = ErrorCode::IllegalTopic;
        break;
      }

      case rmq::Code::CLIENT_ID_REQUIRED: {
        SPDLOG_WARN("ClientIdRequired: {}, host={}", status.message(), peer_address);
        ec = ErrorCode::InternalClientError;
        break;
      }

      case rmq::Code::TOPIC_NOT_FOUND: {
        SPDLOG_WARN("TopicNotFound: {}, host={}", status.message(), peer_address);
        ec = ErrorCode::TopicNotFound;
        break;
      }

      case rmq::Code::UNAUTHORIZED: {
        SPDLOG_WARN("Unauthorized: {}, host={}", status.message(), peer_address);
        ec = ErrorCode::Unauthorized;
        break;
      }

      case rmq::Code::FORBIDDEN: {
        SPDLOG_WARN("Forbidden: {}, host={}", status.message(), peer_address);
        ec = ErrorCode::Forbidden;
        break;
      }

      case rmq::Code::INTERNAL_SERVER_ERROR: {
        SPDLOG_WARN("InternalServerError: {}, host={}", status.message(), peer_address);
        ec = ErrorCode::InternalServerError;
        break;
      }

      case rmq::Code::PROXY_TIMEOUT: {
        SPDLOG_WARN("GatewayTimeout: {}, host={}", status.message(), peer_address);
        ec = ErrorCode::GatewayTimeout;
        break;
      }

      default: {
        SPDLOG_WARN("NotSupported: please upgrade SDK to latest release. {}, host={}", status.message(), peer_address);
        ec = ErrorCode::NotSupported;
        break;
      }
    }
    cb(ec, invocation_context->response);
  };

  invocation_context->callback = callback;
  client->asyncRecallMessage(request, invocation_context);
}

void ClientManagerImpl::forwardMessageToDeadLetterQueue(const std::string& target_host,
                                                        const Metadata& metadata,
                                                        const ForwardMessageToDeadLetterQueueRequest& request,
                                                        std::chrono::milliseconds timeout,
                                                        const std::function<void(const std::error_code&)>& cb) {
  SPDLOG_DEBUG("ForwardMessageToDeadLetterQueue Request: {}", request.DebugString());
  auto client = getRpcClient(target_host);
  auto invocation_context = new InvocationContext<ForwardMessageToDeadLetterQueueResponse>();
  invocation_context->task_name =
      fmt::format("Forward message[{}] to DLQ against {}", request.message_id(), target_host);
  invocation_context->remote_address = target_host;
  invocation_context->context.set_deadline(std::chrono::system_clock::now() + timeout);

  for (const auto& item : metadata) {
    invocation_context->context.AddMetadata(item.first, item.second);
  }

  auto callback = [cb](const InvocationContext<ForwardMessageToDeadLetterQueueResponse>* invocation_context) {
    if (!invocation_context->status.ok()) {
      SPDLOG_WARN("Failed to transmit SendMessageToDeadLetterQueueRequest to host={}",
                  invocation_context->remote_address);
      std::error_code ec = ErrorCode::BadRequest;
      cb(ec);
      return;
    }

    SPDLOG_DEBUG("Received forwardToDeadLetterQueue response from server[host={}]", invocation_context->remote_address);
    std::error_code ec;
    auto&& status = invocation_context->response.status();
    auto&& peer_address = invocation_context->remote_address;
    switch (status.code()) {
      case rmq::Code::OK: {
        break;
      }
      case rmq::Code::ILLEGAL_TOPIC: {
        SPDLOG_WARN("IllegalTopic: {}. Host={}", status.message(), peer_address);
        ec = ErrorCode::IllegalTopic;
        break;
      }

      case rmq::Code::ILLEGAL_CONSUMER_GROUP: {
        SPDLOG_WARN("IllegalConsumerGroup: {}. Host={}", status.message(), peer_address);
        ec = ErrorCode::IllegalConsumerGroup;
        break;
      }

      case rmq::Code::INVALID_RECEIPT_HANDLE: {
        SPDLOG_WARN("IllegalReceiptHandle: {}. Host={}", status.message(), peer_address);
        ec = ErrorCode::InvalidReceiptHandle;
        break;
      }

      case rmq::Code::CLIENT_ID_REQUIRED: {
        SPDLOG_WARN("IllegalTopic: {}. Host={}", status.message(), peer_address);
        ec = ErrorCode::InternalClientError;
        break;
      }

      case rmq::Code::TOPIC_NOT_FOUND: {
        ec = ErrorCode::TopicNotFound;
        break;
      }

      case rmq::Code::INTERNAL_SERVER_ERROR: {
        ec = ErrorCode::ServiceUnavailable;
        break;
      }

      case rmq::Code::TOO_MANY_REQUESTS: {
        ec = ErrorCode::TooManyRequests;
        break;
      }

      case rmq::Code::PROXY_TIMEOUT: {
        ec = ErrorCode::GatewayTimeout;
        break;
      }

      default: {
        ec = ErrorCode::NotImplemented;
        break;
      }
    }
    cb(ec);
  };

  invocation_context->callback = callback;
  client->asyncForwardMessageToDeadLetterQueue(request, invocation_context);
}

std::error_code ClientManagerImpl::notifyClientTermination(const std::string& target_host,
                                                           const Metadata& metadata,
                                                           const NotifyClientTerminationRequest& request,
                                                           std::chrono::milliseconds timeout) {
  std::error_code ec;
  auto client = getRpcClient(target_host);
  if (!client) {
    SPDLOG_WARN("Failed to create RpcClient for host={}", target_host);
    ec = ErrorCode::RequestTimeout;
    return ec;
  }

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + timeout);
  for (const auto& item : metadata) {
    context.AddMetadata(item.first, item.second);
  }

  SPDLOG_DEBUG("NotifyClientTermination request: {}", request.DebugString());

  NotifyClientTerminationResponse response;
  {
    grpc::Status status = client->notifyClientTermination(&context, request, &response);
    if (!status.ok()) {
      SPDLOG_WARN("NotifyClientTermination failed. gRPC-code={}, gRPC-message={}, host={}", status.error_code(),
                  status.error_message(), target_host);
      ec = ErrorCode::RequestTimeout;
      return ec;
    }
  }

  auto&& status = response.status();

  switch (status.code()) {
    case rmq::Code::OK: {
      SPDLOG_DEBUG("NotifyClientTermination OK. host={}", target_host);
      break;
    }

    case rmq::Code::ILLEGAL_CONSUMER_GROUP: {
      SPDLOG_ERROR("IllegalConsumerGroup: {}. Host={}", status.message(), target_host);
      ec = ErrorCode::IllegalConsumerGroup;
      break;
    }

    case rmq::Code::INTERNAL_SERVER_ERROR: {
      SPDLOG_WARN("InternalServerError: Cause={}, host={}", status.message(), target_host);
      ec = ErrorCode::InternalServerError;
      break;
    }

    case rmq::Code::UNAUTHORIZED: {
      SPDLOG_WARN("Unauthorized due to lack of valid authentication credentials: Cause={}, host={}", status.message(),
                  target_host);
      ec = ErrorCode::Unauthorized;
      break;
    }

    case rmq::Code::FORBIDDEN: {
      SPDLOG_WARN("Forbidden due to insufficient permission to the resource: Cause={}, host={}", status.message(),
                  target_host);
      ec = ErrorCode::Forbidden;
      break;
    }

    default: {
      SPDLOG_WARN("NotSupported. Please upgrade to latest SDK release. host={}", target_host);
      ec = ErrorCode::NotSupported;
      break;
    }
  }
  return ec;
}

void ClientManagerImpl::submit(std::function<void()> task) {
  State current_state = state();
  if (current_state == State::STOPPING || current_state == State::STOPPED) {
    return;
  }
  callback_thread_pool_->submit(task);
}

const char* ClientManagerImpl::HEARTBEAT_TASK_NAME = "heartbeat-task";
const char* ClientManagerImpl::STATS_TASK_NAME = "stats-task";

ROCKETMQ_NAMESPACE_END
