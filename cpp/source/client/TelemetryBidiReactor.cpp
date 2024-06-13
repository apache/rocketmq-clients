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
#include "TelemetryBidiReactor.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <utility>

#include "ClientManager.h"
#include "MessageExt.h"
#include "Metadata.h"
#include "RpcClient.h"
#include "Signature.h"
#include "google/protobuf/util/time_util.h"
#include "rocketmq/Logger.h"
#include "spdlog/spdlog.h"

ROCKETMQ_NAMESPACE_BEGIN

TelemetryBidiReactor::TelemetryBidiReactor(std::weak_ptr<Client> client,
                                           rmq::MessagingService::Stub* stub,
                                           std::string peer_address)
    : client_(client), peer_address_(std::move(peer_address)), stream_state_(StreamState::Active) {
  auto ptr = client_.lock();
  auto deadline = std::chrono::system_clock::now() + std::chrono::hours(1);
  context_.set_deadline(deadline);
  Metadata metadata;
  Signature::sign(ptr->config(), metadata);
  for (const auto& entry : metadata) {
    context_.AddMetadata(entry.first, entry.second);
  }
  stub->async()->Telemetry(&context_, this);
  // Increase hold for write stream.
  AddHold();
  StartCall();
}

TelemetryBidiReactor::~TelemetryBidiReactor() {
  SPDLOG_INFO("Telemetry stream for {} destructed. StreamState={}", peer_address_,
              static_cast<std::uint8_t>(stream_state_));
}

bool TelemetryBidiReactor::await() {
  absl::MutexLock lk(&server_setting_received_mtx_);
  if (server_setting_received_) {
    return true;
  }

  server_setting_received_cv_.Wait(&server_setting_received_mtx_);
  return server_setting_received_;
}

void TelemetryBidiReactor::OnWriteDone(bool ok) {
  {
    bool expected = true;
    if (!command_inflight_.compare_exchange_strong(expected, false, std::memory_order_relaxed)) {
      SPDLOG_WARN("Illegal command-inflight state");
      return;
    }
  }

  if (!ok) {
    RemoveHold();
    {
      absl::MutexLock lk(&writes_mtx_);
      SPDLOG_WARN("Failed to write telemetry command {} to {}", writes_.front().DebugString(), peer_address_);
    }

    {
      absl::MutexLock lk(&stream_state_mtx_);
      if (streamStateGood()) {
        stream_state_ = StreamState::WriteFailure;
      }
    }
    return;
  }

  // Check if the read stream has started.
  fireRead();

  // Remove the command that has been written to server.
  {
    absl::MutexLock lk(&writes_mtx_);
    writes_.pop_front();
  }

  tryWriteNext();
}

void TelemetryBidiReactor::OnReadDone(bool ok) {
  if (!ok) {
    RemoveHold();
    {
      absl::MutexLock lk(&stream_state_mtx_);
      if (!ok) {
        if (streamStateGood()) {
          stream_state_ = StreamState::ReadFailure;
          SPDLOG_WARN("Faild to read from telemetry stream from {}", peer_address_);
        }
        return;
      }
    }
    return;
  }

  SPDLOG_DEBUG("Read a telemetry command from {}: {}", peer_address_, read_.DebugString());
  auto ptr = client_.lock();
  if (!ptr) {
    SPDLOG_INFO("Client for {} has destructed", peer_address_);
    return;
  }

  switch (read_.command_case()) {
    case rmq::TelemetryCommand::kSettings: {
      auto settings = read_.settings();
      SPDLOG_INFO("Received settings from {}: {}", peer_address_, settings.DebugString());
      applySettings(settings);
      {
        absl::MutexLock lk(&server_setting_received_mtx_);
        if (!server_setting_received_) {
          server_setting_received_ = true;
          server_setting_received_cv_.SignalAll();
        }
      }
      break;
    }
    case rmq::TelemetryCommand::kRecoverOrphanedTransactionCommand: {
      auto client = client_.lock();
      if (!client) {
        fireClose();
        return;
      }
      SPDLOG_DEBUG("Receive orphan transaction command: {}", read_.DebugString());
      auto message = client->manager()->wrapMessage(read_.release_verify_message_command()->message());
      auto raw = const_cast<Message*>(message.get());
      raw->mutableExtension().target_endpoint = peer_address_;
      raw->mutableExtension().transaction_id = read_.recover_orphaned_transaction_command().transaction_id();
      client->recoverOrphanedTransaction(message);

      break;
    }

    case rmq::TelemetryCommand::kPrintThreadStackTraceCommand: {
      TelemetryCommand response;
      response.mutable_thread_stack_trace()->set_nonce(read_.print_thread_stack_trace_command().nonce());
      response.mutable_thread_stack_trace()->set_thread_stack_trace("PrintStackTrace is not supported");
      write(std::move(response));
      break;
    }

    case rmq::TelemetryCommand::kVerifyMessageCommand: {
      auto client = client_.lock();
      if (!client) {
        fireClose();
        return;
      }

      std::weak_ptr<TelemetryBidiReactor> ptr(shared_from_this());
      auto cb = [ptr](TelemetryCommand command) {
        auto reactor = ptr.lock();
        if (!reactor) {
          return;
        }
        reactor->onVerifyMessageResult(std::move(command));
      };
      auto message = client->manager()->wrapMessage(read_.verify_message_command().message());
      auto raw = const_cast<Message*>(message.get());
      raw->mutableExtension().target_endpoint = peer_address_;
      raw->mutableExtension().nonce = read_.verify_message_command().nonce();
      client->verify(message, cb);
      break;
    }

    default: {
      SPDLOG_WARN("Unsupported command");
      break;
    }
  }

  StartRead(&read_);
}

void TelemetryBidiReactor::applySettings(const rmq::Settings& settings) {
  auto ptr = client_.lock();
  if (!ptr) {
    SPDLOG_INFO("Client for {} has destructed", peer_address_);
    return;
  }

  applyBackoffPolicy(settings, ptr);

  // Sync metrics collector configuration
  if (settings.has_metric()) {
    const auto& metric = settings.metric();
    ptr->config().metric.on = metric.on();
    ptr->config().metric.endpoints.set_scheme(metric.endpoints().scheme());
    ptr->config().metric.endpoints.mutable_addresses()->CopyFrom(metric.endpoints().addresses());
  }

  switch (settings.pub_sub_case()) {
    case rmq::Settings::PubSubCase::kPublishing: {
      applyPublishingConfig(settings, ptr);
      break;
    }
    case rmq::Settings::PubSubCase::kSubscription: {
      applySubscriptionConfig(settings, ptr);
      break;
    }
    default: {
      break;
    }
  }
}

void TelemetryBidiReactor::applyBackoffPolicy(const rmq::Settings& settings, std::shared_ptr<Client>& ptr) {
  // Apply backoff policy on throttling
  if (settings.has_backoff_policy()) {
    const auto& backoff_policy = settings.backoff_policy();
    ptr->config().backoff_policy.max_attempt = backoff_policy.max_attempts();
    if (backoff_policy.has_customized_backoff()) {
      for (const auto& item : backoff_policy.customized_backoff().next()) {
        auto backoff = std::chrono::seconds(item.seconds()) + std::chrono::nanoseconds(item.nanos());
        ptr->config().backoff_policy.next.push_back(absl::FromChrono(backoff));
      }
    }

    if (backoff_policy.has_exponential_backoff()) {
      const auto& exp_backoff = backoff_policy.exponential_backoff();
      ptr->config().backoff_policy.initial = absl::FromChrono(std::chrono::seconds(exp_backoff.initial().seconds()) +
                                                              std::chrono::nanoseconds(exp_backoff.initial().nanos()));
      ptr->config().backoff_policy.multiplier = exp_backoff.multiplier();
      ptr->config().backoff_policy.max = absl::FromChrono(std::chrono::seconds(exp_backoff.max().seconds()) +
                                                          std::chrono::nanoseconds(exp_backoff.max().nanos()));
    }
  }
}

void TelemetryBidiReactor::applyPublishingConfig(const rmq::Settings& settings, std::shared_ptr<Client> client) {
  // The server may have implicitly assumed a namespace for the client.
  if (!settings.publishing().topics().empty()) {
    for (const auto& topic : settings.publishing().topics()) {
      if (topic.resource_namespace() != client->config().resource_namespace) {
        SPDLOG_INFO("Client namespace is changed from [{}] to [{}]", client->config().resource_namespace,
                    topic.resource_namespace());
        client->config().resource_namespace = topic.resource_namespace();
        break;
      }
    }
  }
  client->config().publisher.max_body_size = settings.publishing().max_body_size();
}

void TelemetryBidiReactor::applySubscriptionConfig(const rmq::Settings& settings, std::shared_ptr<Client> client) {
  // The server may have implicitly assumed a namespace for the client.
  if (!settings.subscription().subscriptions().empty()) {
    for (const auto& subscription : settings.subscription().subscriptions()) {
      if (subscription.topic().resource_namespace() != client->config().resource_namespace) {
        SPDLOG_INFO("Client namespace is changed from [{}] to [{}]", client->config().resource_namespace,
                    subscription.topic().resource_namespace());
        client->config().resource_namespace = subscription.topic().resource_namespace();
        break;
      }
    }
  }

  client->config().subscriber.fifo = settings.subscription().fifo();
  auto polling_timeout =
      google::protobuf::util::TimeUtil::DurationToMilliseconds(settings.subscription().long_polling_timeout());
  client->config().subscriber.polling_timeout = absl::Milliseconds(polling_timeout);
  client->config().subscriber.receive_batch_size = settings.subscription().receive_batch_size();
}

void TelemetryBidiReactor::fireRead() {
  {
    absl::MutexLock lk(&stream_state_mtx_);
    if (!streamStateGood()) {
      SPDLOG_WARN("Further read from {} is not allowded due to stream-state={}", peer_address_,
                  static_cast<std::uint8_t>(stream_state_));
      return;
    }
  }

  bool expected = false;
  if (read_stream_started_.compare_exchange_weak(expected, true, std::memory_order_relaxed)) {
    // Hold for the read stream.
    AddHold();
    StartRead(&read_);
  }
}

void TelemetryBidiReactor::write(TelemetryCommand command) {
  {
    absl::MutexLock lk(&stream_state_mtx_);
    // Reject incoming write commands if the stream state is closing or has witnessed some error.
    if (!streamStateGood() || StreamState::Closing == stream_state_) {
      return;
    }
  }

  {
    absl::MutexLock lk(&writes_mtx_);
    writes_.push_back(command);
  }
  tryWriteNext();
}

void TelemetryBidiReactor::tryWriteNext() {
  {
    absl::MutexLock lk(&stream_state_mtx_);
    if (!streamStateGood()) {
      SPDLOG_WARN("Further write to {} is not allowded due to stream-state={}", peer_address_,
                  static_cast<std::uint8_t>(stream_state_));
      return;
    }
  }

  bool closing = false;
  {
    absl::MutexLock lk(&stream_state_mtx_);
    if (StreamState::Closing == stream_state_) {
      closing = true;
    }
  }

  {
    absl::MutexLock lk(&writes_mtx_);
    if (writes_.empty() && !closing) {
      SPDLOG_DEBUG("No TelemtryCommand to write. Peer={}", peer_address_);
      return;
    }

    bool expected = false;
    if (command_inflight_.compare_exchange_strong(expected, true, std::memory_order_relaxed)) {
      if (writes_.empty()) {
        // Tell server there is no more write requests.
        StartWritesDone();
      } else {
        SPDLOG_DEBUG("Writing telemetry command to {}: {}", peer_address_, writes_.front().DebugString());
        StartWrite(&(writes_.front()));
      }
    } else {
      SPDLOG_DEBUG("Another command is already on the wire. Peer={}", peer_address_);
      return;
    }
  }
}

void TelemetryBidiReactor::fireClose() {
  SPDLOG_INFO("{}#fireClose", peer_address_);

  {
    absl::MutexLock lk(&stream_state_mtx_);
    if (!streamStateGood()) {
      SPDLOG_WARN("No futher Read/Write call to {} is allowed due to stream-state={}", peer_address_,
                  static_cast<std::uint8_t>(stream_state_));
      return;
    }
  }

  {
    absl::MutexLock lk(&stream_state_mtx_);
    stream_state_ = StreamState::Closing;
  }
  tryWriteNext();
}

void TelemetryBidiReactor::OnWritesDoneDone(bool ok) {
  // Remove the hold for the write stream.
  RemoveHold();

  if (!ok) {
    absl::MutexLock lk(&stream_state_mtx_);
    if (streamStateGood()) {
      stream_state_ = StreamState::WriteFailure;
    }
    SPDLOG_WARN("Previous telemetry write to {} failed", peer_address_);
    return;
  }

  SPDLOG_DEBUG("{}#OnWritesDoneDone", peer_address_);
}

void TelemetryBidiReactor::onVerifyMessageResult(TelemetryCommand command) {
  {
    absl::MutexLock lk(&writes_mtx_);
    writes_.emplace_back(command);
  }
  tryWriteNext();
}

/// Notifies the application that all operations associated with this RPC
/// have completed and all Holds have been removed. OnDone provides the RPC
/// status outcome for both successful and failed RPCs and will be called in
/// all cases. If it is not called, it indicates an application-level problem
/// (like failure to remove a hold).
///
/// \param[in] status The status outcome of this RPC
void TelemetryBidiReactor::OnDone(const grpc::Status& status) {
  SPDLOG_DEBUG("{}#OnDone, status.ok={}", peer_address_, status.ok());
  if (!status.ok()) {
    SPDLOG_WARN("{}#OnDone, status.error_code={}, status.error_message={}, status.error_details={}", peer_address_,
                status.error_code(), status.error_message(), status.error_details());
  }

  {
    SPDLOG_DEBUG("{} notifies awaiting close call", peer_address_);
    absl::MutexLock lk(&stream_state_mtx_);
    if (streamStateGood()) {
      stream_state_ = StreamState::Closed;
    }

    stream_state_cv_.SignalAll();
  }

  auto client = client_.lock();
  if (!client) {
    return;
  }

  if (client->active()) {
    client->createSession(peer_address_, true);
  }
}

void TelemetryBidiReactor::OnReadInitialMetadataDone(bool ok) {
  if (!ok) {
    absl::MutexLock lk(&stream_state_mtx_);
    if (streamStateGood()) {
      stream_state_ = StreamState::ReadInitialMetadataFailure;
    }
    SPDLOG_WARN("Read of initial-metadata failed from {}", peer_address_);
    return;
  }

  SPDLOG_DEBUG("Received initial metadata from {}", peer_address_);
}

bool TelemetryBidiReactor::streamStateGood() {
  return StreamState::Active == stream_state_ || StreamState::Closing == stream_state_;
}

ROCKETMQ_NAMESPACE_END