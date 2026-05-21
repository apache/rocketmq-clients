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
#include <future>
#include <list>
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
  Ready = 0,
  Closing = 1,
  Closed = 2,
};

/// TelemetryBidiReactor: Manages a bidirectional gRPC stream for telemetry data
///
/// Stream State Transitions:
///    Ready --> Closing --> Closed
///
/// Key Features:
///    1. Close Operation: Performs a blocking wait until the bidirectional reactor is fully closed.
///    2. Session Management: If the session closes while the client is still active,
///       it automatically initiates the creation of a new session to maintain
///       communication with the server.
///
/// The reactor handles reading from and writing to the stream, manages stream state,
/// and applies settings received from the server.
class TelemetryBidiReactor : public grpc::ClientBidiReactor<TelemetryCommand, TelemetryCommand>,
                             public std::enable_shared_from_this<TelemetryBidiReactor> {
public:
  TelemetryBidiReactor(std::weak_ptr<Client> client, rmq::MessagingService::Stub* stub, std::string peer_address);

  ~TelemetryBidiReactor() override;

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

  /// Core API method to initiate this bidirectional stream.
  void write(TelemetryCommand command);

  bool awaitApplyingSettings();

  void close();

private:
  grpc::ClientContext context_;

  /**
   * @brief Command to read from server.
   */
  TelemetryCommand read_;

  /**
   * @brief Buffered commands to write to server
   *
   * TODO: move buffered commands to a shared container, which may survive
   * multiple TelemetryBidiReactor lifecycles.
   */
  std::list<TelemetryCommand> writes_ GUARDED_BY(writes_mtx_);
  absl::Mutex writes_mtx_;

  /**
   * @brief Each TelemetryBidiReactor belongs to a specific client as its owner.
   */
  std::weak_ptr<Client> client_;

  /**
   * @brief Address of remote peer.
   */
  std::string peer_address_;

  StreamState state_ GUARDED_BY(state_mtx_);
  absl::Mutex state_mtx_;
  absl::CondVar state_cv_;

  std::promise<bool> sync_settings_promise_;

  void applySettings(const rmq::Settings& settings);

  void applyBackoffPolicy(const rmq::Settings& settings, std::shared_ptr<Client>& client);

  void applyPublishingConfig(const rmq::Settings& settings, std::shared_ptr<Client> client);

  void applySubscriptionConfig(const rmq::Settings& settings, std::shared_ptr<Client> client);

  /// Attempt to write pending telemetry command to server.
  void tryWriteNext() LOCKS_EXCLUDED(state_mtx_, writes_mtx_);

  void signalClose();
};

ROCKETMQ_NAMESPACE_END
