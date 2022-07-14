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
#include <memory>
#include <utility>
#include <vector>

#include "Client.h"
#include "RpcClient.h"
#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

enum class StreamState : std::uint8_t
{
  Created = 0,
  Active = 1,
  ReadDone = 2,
  WriteDone = 3,
  Closed = 4,
};

class TelemetryBidiReactor : public grpc::ClientBidiReactor<TelemetryCommand, TelemetryCommand>,
                             public std::enable_shared_from_this<TelemetryBidiReactor> {
public:
  TelemetryBidiReactor(std::weak_ptr<Client> client, rmq::MessagingService::Stub* stub, std::string peer_address);

  ~TelemetryBidiReactor();

  void OnWriteDone(bool ok) override;

  void OnWritesDoneDone(bool ok) override;

  void OnReadDone(bool ok) override;

  void OnDone(const grpc::Status& status) override;

  void fireRead();

  void fireWrite();

  void fireClose();

  void write(TelemetryCommand command);

  bool await();

private:
  grpc::ClientContext context_;

  /**
   * @brief Command to read from server.
   */
  TelemetryCommand read_;

  /**
   * @brief Buffered commands to write to server
   */
  std::vector<TelemetryCommand> writes_ GUARDED_BY(writes_mtx_);
  absl::Mutex writes_mtx_;

  /**
   * @brief The command that is currently being written back to server.
   */
  TelemetryCommand write_;

  /**
   * @brief Each TelemetryBidiReactor belongs to a specific client as its owner. 
   */
  std::weak_ptr<Client> client_;

  /**
   * @brief Address of remote peer.
   */
  std::string peer_address_;

  /**
   * @brief Indicate if there is a command being written to network.
   */
  std::atomic_bool command_inflight_{false};

  StreamState stream_state_ GUARDED_BY(stream_state_mtx_);
  absl::Mutex stream_state_mtx_;
  absl::CondVar stream_state_cv_;

  bool server_setting_received_ GUARDED_BY(server_setting_received_mtx_){false};
  absl::Mutex server_setting_received_mtx_;
  absl::CondVar server_setting_received_cv_;

  void onVerifyMessageResult(TelemetryCommand command);

  void applySettings(const rmq::Settings& settings);

  void applyBackoffPolicy(const rmq::Settings& settings, std::shared_ptr<Client>& client);

  void applyPublishingConfig(const rmq::Settings& settings, std::shared_ptr<Client> client);

  void applySubscriptionConfig(const rmq::Settings& settings, std::shared_ptr<Client> client);
};

ROCKETMQ_NAMESPACE_END