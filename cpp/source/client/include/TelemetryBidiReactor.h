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
  Active = 0,

  // Once stream state reaches one of the following, Start* should not be called.
  Closed = 1,
  ReadInitialMetadataFailure = 2,
  ReadFailure = 3,
  WriteFailure = 4,
};

class TelemetryBidiReactor : public grpc::ClientBidiReactor<TelemetryCommand, TelemetryCommand>,
                             public std::enable_shared_from_this<TelemetryBidiReactor> {
public:
  TelemetryBidiReactor(std::weak_ptr<Client> client, rmq::MessagingService::Stub* stub, std::string peer_address);

  ~TelemetryBidiReactor();

  /// Notifies the application that all operations associated with this RPC
  /// have completed and all Holds have been removed. OnDone provides the RPC
  /// status outcome for both successful and failed RPCs and will be called in
  /// all cases. If it is not called, it indicates an application-level problem
  /// (like failure to remove a hold).
  ///
  /// \param[in] s The status outcome of this RPC
  void OnDone(const grpc::Status& status) override;

  /// Notifies the application that a read of initial metadata from the
  /// server is done. If the application chooses not to implement this method,
  /// it can assume that the initial metadata has been read before the first
  /// call of OnReadDone or OnDone.
  ///
  /// \param[in] ok Was the initial metadata read successfully? If false, no
  ///               new read/write operation will succeed, and any further
  ///               Start* operations should not be called.
  void OnReadInitialMetadataDone(bool /*ok*/) override;

  /// Notifies the application that a StartRead operation completed.
  ///
  /// \param[in] ok Was it successful? If false, no new read/write operation
  ///               will succeed, and any further Start* should not be called.
  void OnReadDone(bool ok) override;

  /// Notifies the application that a StartWrite or StartWriteLast operation
  /// completed.
  ///
  /// \param[in] ok Was it successful? If false, no new read/write operation
  ///               will succeed, and any further Start* should not be called.
  void OnWriteDone(bool ok) override;

  /// Notifies the application that a StartWritesDone operation completed. Note
  /// that this is only used on explicit StartWritesDone operations and not for
  /// those that are implicitly invoked as part of a StartWriteLast.
  ///
  /// \param[in] ok Was it successful? If false, the application will later see
  ///               the failure reflected as a bad status in OnDone and no
  ///               further Start* should be called.
  void OnWritesDoneDone(bool ok) override;

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
   *
   * TODO: move buffered commands to a shared container, which may survive multiple TelemetryBidiReactor lifecycles.
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

  /**
   * Indicate if the underlying gRPC bidirectional stream is good enough to fire further Start* calls.
   */
  bool streamStateGood() ABSL_EXCLUSIVE_LOCKS_REQUIRED(stream_state_mtx_);
};

ROCKETMQ_NAMESPACE_END