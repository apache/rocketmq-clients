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
#include <functional>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

#include "Client.h"
#include "ClientManager.h"
#include "InsecureCertificateVerifier.h"
#include "ReceiveMessageCallback.h"
#include "RpcClientImpl.h"
#include "ThreadPoolImpl.h"
#include "TopicRouteData.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "rocketmq/State.h"

ROCKETMQ_NAMESPACE_BEGIN

class ClientManagerImpl : virtual public ClientManager, public std::enable_shared_from_this<ClientManagerImpl> {
public:
  /**
   * @brief Construct a new Client Manager Impl object
   * TODO: Make it protected such that instantiating it through ClientManagerFactory only, achieving Singleton
   * effectively.
   * @param resource_namespace Abstract resource namespace, in which this client manager lives.
   */
  explicit ClientManagerImpl(std::string resource_namespace, bool with_ssl = true);

  ~ClientManagerImpl() override;

  void start() override;

  void shutdown() override LOCKS_EXCLUDED(rpc_clients_mtx_);

  std::shared_ptr<grpc::Channel> createChannel(const std::string& target_host) override;

  /**
   * Resolve route data from name server for the given topic.
   *
   * @param target_host Name server host address;
   * @param metadata Request headers;
   * @param request Query route entries request.
   * @param timeout RPC timeout.
   * @param cb Callback to execute once the request/response completes.
   */
  void resolveRoute(const std::string& target_host,
                    const Metadata& metadata,
                    const QueryRouteRequest& request,
                    std::chrono::milliseconds timeout,
                    const std::function<void(const std::error_code&, const TopicRouteDataPtr&)>& cb) override
      LOCKS_EXCLUDED(rpc_clients_mtx_);

  /**
   * If inactive RPC clients refer to remote hosts that are absent from topic_route_table_, we need to purge them
   * immediately.
   */
  std::vector<std::string> cleanOfflineRpcClients() LOCKS_EXCLUDED(clients_mtx_, rpc_clients_mtx_);

  bool send(const std::string& target_host,
            const Metadata& metadata,
            SendMessageRequest& request,
            SendResultCallback cb) override LOCKS_EXCLUDED(rpc_clients_mtx_);

  /**
   * Get a RpcClient according to the given target hosts, which follows scheme specified
   * https://github.com/grpc/grpc/blob/master/doc/naming.md
   *
   * Note that a channel in gRPC is composted of one or more sub-channels. Every sub-channel represents a solid TCP
   * connection. gRPC supports a number of configurable load-balancing policy with "pick-first" as the default option.
   * Requests are distributed
   * @param target_host
   * @param need_heartbeat
   * @return
   */
  RpcClientSharedPtr getRpcClient(const std::string& target_host, bool need_heartbeat = true) override
      LOCKS_EXCLUDED(rpc_clients_mtx_);

  static SendResult processSendResponse(const rmq::MessageQueue& message_queue,
                                         const SendMessageResponse& response,
                                         std::error_code& ec);

  // only for test
  void addRpcClient(const std::string& target_host, const RpcClientSharedPtr& client) LOCKS_EXCLUDED(rpc_clients_mtx_);

  // Test purpose only
  void cleanRpcClients() LOCKS_EXCLUDED(rpc_clients_mtx_);

  void addClientObserver(std::weak_ptr<Client> client) override;

  void queryAssignment(const std::string& target,
                       const Metadata& metadata,
                       const QueryAssignmentRequest& request,
                       std::chrono::milliseconds timeout,
                       const std::function<void(const std::error_code&, const QueryAssignmentResponse&)>& cb) override;

  void receiveMessage(const std::string& target,
                      const Metadata& metadata,
                      const ReceiveMessageRequest& request,
                      std::chrono::milliseconds timeout,
                      ReceiveMessageCallback cb) override LOCKS_EXCLUDED(rpc_clients_mtx_);

  /**
   * Translate protobuf message struct to domain model.
   *
   * @param item
   * @return true if the translation succeeded; false if something wrong happens, including checksum verification, etc.
   */
  MessageConstSharedPtr wrapMessage(const rmq::Message& item) override;

  SchedulerSharedPtr getScheduler() override;

  /**
   * Ack message asynchronously.
   * @param target_host Target broker host address.
   * @param request Ack message request.
   */
  void ack(const std::string& target_host,
           const Metadata& metadata,
           const AckMessageRequest& request,
           std::chrono::milliseconds timeout,
           const std::function<void(const std::error_code&)>& cb) override;

  void changeInvisibleDuration(const std::string& target_host,
                               const Metadata& metadata,
                               const ChangeInvisibleDurationRequest&,
                               std::chrono::milliseconds timeout,
                               const std::function<void(const std::error_code&, const ChangeInvisibleDurationResponse&)>&) override;

  void forwardMessageToDeadLetterQueue(const std::string& target_host,
                                       const Metadata& metadata,
                                       const ForwardMessageToDeadLetterQueueRequest& request,
                                       std::chrono::milliseconds timeout,
                                       const std::function<void(const std::error_code&)>& cb) override;

  /**
   * End a transaction asynchronously.
   *
   * Callback conforms the following method signature:
   * void callable(bool rpc_ok, const EndTransactionResponse& response)
   *
   * if rpc_ok is false, the end transaction request may never reach the server and consequently no need to inspect the
   * response. If rpc_ok is true, response should be further inspected to determine business tier code and logic.
   * @param target_host
   * @param metadata
   * @param request
   * @param timeout
   * @param cb
   */
  void endTransaction(const std::string& target_host,
                      const Metadata& metadata,
                      const EndTransactionRequest& request,
                      std::chrono::milliseconds timeout,
                      const std::function<void(const std::error_code&, const EndTransactionResponse&)>& cb) override;

  void recallMessage(const std::string& target_host,
                     const Metadata& metadata,
                     const RecallMessageRequest& request,
                     std::chrono::milliseconds timeout,
                     const std::function<void(const std::error_code&, const RecallMessageResponse&)>& cb) override;

  std::error_code notifyClientTermination(const std::string& target_host,
                                          const Metadata& metadata,
                                          const NotifyClientTerminationRequest& request,
                                          std::chrono::milliseconds timeout) override;

  void trace(bool trace) {
    trace_ = trace;
  }

  void heartbeat(const std::string& target_host,
                 const Metadata& metadata,
                 const HeartbeatRequest& request,
                 std::chrono::milliseconds timeout,
                 const std::function<void(const std::error_code&, const HeartbeatResponse&)>& cb) override;

  State state() const override;

  void submit(std::function<void()> task) override;

private:
  void doHeartbeat();

  SchedulerSharedPtr scheduler_;

  static const char* HEARTBEAT_TASK_NAME;
  static const char* STATS_TASK_NAME;

  std::string resource_namespace_;

  std::atomic<State> state_;

  std::vector<std::weak_ptr<Client>> clients_ GUARDED_BY(clients_mtx_);
  absl::Mutex clients_mtx_;

  absl::flat_hash_map<std::string, std::shared_ptr<RpcClient>> rpc_clients_ GUARDED_BY(rpc_clients_mtx_);
  absl::Mutex rpc_clients_mtx_;  // protects rpc_clients_

  std::uint32_t heartbeat_task_id_{0};

  std::unique_ptr<ThreadPoolImpl> callback_thread_pool_;

  absl::flat_hash_set<std::string> exporter_endpoint_set_ GUARDED_BY(exporter_endpoint_set_mtx_);
  absl::Mutex exporter_endpoint_set_mtx_;

  /**
   * Tenant-id. Each user shall have one unique identifier.
   */
  std::string tenant_id_;

  std::string service_name_{"RocketMQ"};

  /**
   * TLS configuration
   */
  std::shared_ptr<grpc::experimental::CertificateVerifier> certificate_verifier_;
  grpc::experimental::TlsChannelCredentialsOptions tls_channel_credential_options_;
  std::shared_ptr<grpc::ChannelCredentials> channel_credential_;
  grpc::ChannelArguments channel_arguments_;

  bool trace_{false};
  bool with_ssl_;
};

ROCKETMQ_NAMESPACE_END