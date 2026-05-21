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

#include <memory>
#include <utility>

#include "ClientManager.h"
#include "MessageExt.h"
#include "Metadata.h"
#include "Signature.h"
#include "google/protobuf/util/time_util.h"
#include "rocketmq/Logger.h"
#include "spdlog/spdlog.h"

ROCKETMQ_NAMESPACE_BEGIN

TelemetryBidiReactor::TelemetryBidiReactor(std::weak_ptr<Client> client,
                                           rmq::MessagingService::Stub* stub,
                                           std::string peer_address)
    : client_(client),
      peer_address_(std::move(peer_address)),
      state_(StreamState::Ready) {
  auto ptr = client_.lock();
  auto deadline = std::chrono::system_clock::now() + std::chrono::hours(1);
  context_.set_deadline(deadline);
  Metadata metadata;
  Signature::sign(ptr->config(), metadata);
  for (const auto& entry : metadata) {
    context_.AddMetadata(entry.first, entry.second);
  }
  stub->async()->Telemetry(&context_, this);
  StartRead(&read_);
  // for read stream
  AddHold();
  StartCall();
}

TelemetryBidiReactor::~TelemetryBidiReactor() {
  SPDLOG_INFO("Telemetry stream for {} destructed. StreamState={}", peer_address_, static_cast<std::uint8_t>(state_));
}

bool TelemetryBidiReactor::awaitApplyingSettings() {
  auto settings_future = sync_settings_promise_.get_future();
  std::future_status status = settings_future.wait_for(std::chrono::seconds(3));
  if (status == std::future_status::ready) {
    if (settings_future.get()) {
      return true;
    }
  }
  {
    absl::MutexLock lk(&state_mtx_);
    state_ = StreamState::Closed;
    state_cv_.SignalAll();
  }
  return false;
}

void TelemetryBidiReactor::OnWriteDone(bool ok) {
  SPDLOG_DEBUG("{}#OnWriteDone", peer_address_);

  // for write stream
  RemoveHold();

  if (!ok) {
    SPDLOG_WARN("Failed to write telemetry command {} to {}", writes_.front().ShortDebugString(), peer_address_);
    signalClose();
    return;
  }

  // Remove the command that has been written to server.
  {
    absl::MutexLock lk(&writes_mtx_);
    if (!writes_.empty()) {
      writes_.pop_front();
    }
  }

  tryWriteNext();
}

void TelemetryBidiReactor::OnReadDone(bool ok) {
  SPDLOG_DEBUG("{}#OnReadDone", peer_address_);
  if (!ok) {
    // for read stream
    RemoveHold();
    // SPDLOG_WARN("Failed to read from telemetry stream from {}", peer_address_);
    signalClose();
    return;
  }

  {
    absl::MutexLock lk(&state_mtx_);
    if (StreamState::Ready != state_) {
      return;
    }
  }

  SPDLOG_DEBUG("Read a telemetry command from {}: {}", peer_address_, read_.ShortDebugString());
  auto client = client_.lock();
  if (!client) {
    SPDLOG_INFO("Client for {} has destructed", peer_address_);
    signalClose();
    return;
  }

  switch (read_.command_case()) {
    case rmq::TelemetryCommand::kSettings: {
      auto settings = read_.settings();
      SPDLOG_INFO("Receive settings from {}: {}", peer_address_, settings.ShortDebugString());
      applySettings(settings);
      sync_settings_promise_.set_value(true);
      break;
    }

    case rmq::TelemetryCommand::kRecoverOrphanedTransactionCommand: {
      SPDLOG_INFO("Receive orphan transaction command: {}", read_.ShortDebugString());
      auto message = client->manager()->wrapMessage(
          read_.recover_orphaned_transaction_command().message());
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
      std::weak_ptr<TelemetryBidiReactor> ptr(shared_from_this());
      auto cb = [ptr](TelemetryCommand command) {
        auto reactor = ptr.lock();
        if (!reactor) {
          return;
        }
        reactor->write(std::move(command));
      };
      auto message = client->manager()->wrapMessage(read_.verify_message_command().message());
      auto raw = const_cast<Message*>(message.get());
      raw->mutableExtension().target_endpoint = peer_address_;
      raw->mutableExtension().nonce = read_.verify_message_command().nonce();
      client->verify(message, cb);
      break;
    }

    default: {
      SPDLOG_WARN("Telemetry command receive unsupported command");
      break;
    }
  }

  {
    absl::MutexLock lk(&state_mtx_);
    if (StreamState::Ready == state_) {
      SPDLOG_DEBUG("Spawn new read op, state={}", static_cast<std::uint8_t>(state_));
      StartRead(&read_);
    }
  }
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

void TelemetryBidiReactor::write(TelemetryCommand command) {
  SPDLOG_DEBUG("{}#write", peer_address_);
  {
    absl::MutexLock lk(&state_mtx_);
    // Reject incoming write commands if the stream state is closing or has witnessed some error.
    if (StreamState::Ready != state_) {
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
  SPDLOG_DEBUG("{}#tryWriteNext", peer_address_);
  absl::MutexLock lk(&writes_mtx_);
  if (StreamState::Ready != state_) {
    SPDLOG_WARN("Further write to {} is not allowed due to stream-state={}", peer_address_,
                static_cast<std::uint8_t>(state_));
    return;
  }

  if (writes_.empty()) {
    SPDLOG_DEBUG("No pending TelemetryCommand to write. Peer={}", peer_address_);
    return;
  }

  if (!writes_.empty()) {
    SPDLOG_DEBUG("Writing TelemetryCommand to {}: {}", peer_address_, writes_.front().ShortDebugString());
    if (StreamState::Ready == state_) {
      AddHold();
      StartWrite(&(writes_.front()));
    } else {
      SPDLOG_WARN("Writing TelemetryCommand error due to unexpected state. State={}, Peer={}",
                  static_cast<uint8_t>(state_), peer_address_);
    }
  }
}

void TelemetryBidiReactor::signalClose() {
  absl::MutexLock lk(&state_mtx_);
  if (state_ == StreamState::Ready) {
    state_ = StreamState::Closing;
  }
}

void TelemetryBidiReactor::close() {
  SPDLOG_DEBUG("{}#fireClose", peer_address_);

  {
    absl::MutexLock lk(&state_mtx_);
    if (state_ == StreamState::Ready) {
      state_ = StreamState::Closing;
    }
  }

  {
    absl::MutexLock lk(&writes_mtx_);
    writes_.clear();
  }
  context_.TryCancel();

  // Acquire state lock
  while (StreamState::Closed != state_) {
    absl::MutexLock lk(&state_mtx_);
    if (state_cv_.WaitWithTimeout(&state_mtx_, absl::Seconds(1))) {
      SPDLOG_WARN("StreamState CondVar timed out before getting signalled: state={}",
                  static_cast<uint8_t>(state_));
    }
  }
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
    SPDLOG_DEBUG("{}#OnDone, status.error_code={}, status.error_message={}, status.error_details={}", peer_address_,
                status.error_code(), status.error_message(), status.error_details());
  }
  {
    absl::MutexLock lk(&state_mtx_);
    state_ = StreamState::Closed;
    state_cv_.SignalAll();
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
  SPDLOG_DEBUG("{}#OnReadInitialMetadataDone", peer_address_);

  if (!ok) {
    // for read stream
    // Remove the hold corresponding to AddHold in TelemetryBidiReactor::TelemetryBidiReactor.
    // RemoveHold();

    SPDLOG_DEBUG("Change state {} --> {}", static_cast<std::uint8_t>(state_),
                 static_cast<std::uint8_t>(StreamState::Closing));
    SPDLOG_WARN("Read of initial-metadata failed from {}", peer_address_);
    signalClose();
    return;
  }

  SPDLOG_DEBUG("Received initial metadata from {}", peer_address_);
}

ROCKETMQ_NAMESPACE_END
