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
    : client_(client),
      peer_address_(std::move(peer_address)),
      read_state_(StreamState::Created),
      write_state_(StreamState::Created) {
  auto ptr = client_.lock();
  auto deadline = std::chrono::system_clock::now() + std::chrono::hours(1);
  context_.set_deadline(deadline);
  Metadata metadata;
  Signature::sign(ptr->config(), metadata);
  for (const auto& entry : metadata) {
    context_.AddMetadata(entry.first, entry.second);
  }
  stub->async()->Telemetry(&context_, this);
  write_state_ = StreamState::Ready;
  // Increase hold for write stream.
  AddHold();
  StartCall();
}

TelemetryBidiReactor::~TelemetryBidiReactor() {
  SPDLOG_INFO("Telemetry stream for {} destructed. ReadStreamState={}, WriteStreamState={}", peer_address_,
              static_cast<std::uint8_t>(read_state_), static_cast<std::uint8_t>(read_state_));
}

bool TelemetryBidiReactor::await() {
  absl::MutexLock lk(&state_mtx_);
  if (StreamState::Created != write_state_) {
    return true;
  }

  state_cv_.Wait(&state_mtx_);
  return StreamState::Error != write_state_;
}

void TelemetryBidiReactor::OnWriteDone(bool ok) {
  SPDLOG_DEBUG("{}#OnWriteDone", peer_address_);

  if (!ok) {
    RemoveHold();
    {
      absl::MutexLock lk(&state_mtx_);
      SPDLOG_WARN("Failed to write telemetry command {} to {}", writes_.front().DebugString(), peer_address_);
      write_state_ = StreamState::Error;

      // Sync read state.
      switch (read_state_) {
        case StreamState::Created:
        case StreamState::Ready: {
          SPDLOG_DEBUG("Change read-state {} --> {}", static_cast<std::uint8_t>(read_state_),
                       static_cast<std::uint8_t>(StreamState::Closed));
          read_state_ = StreamState::Closed;
          break;
        }
        case StreamState::Inflight: {
          SPDLOG_DEBUG("Change read-state {} --> {}", static_cast<std::uint8_t>(read_state_),
                       static_cast<std::uint8_t>(StreamState::Closing));
          read_state_ = StreamState::Closing;
          break;
        }
        case StreamState::Closing:
        case StreamState::Error:
        case StreamState::Closed: {
          break;
        }
      }

      state_cv_.SignalAll();
    }
    return;
  } else {
    absl::MutexLock lk(&state_mtx_);
    if (StreamState::Inflight == write_state_) {
      write_state_ = StreamState::Ready;
    }
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
  SPDLOG_DEBUG("{}#OnReadDone", peer_address_);
  {
    absl::MutexLock lk(&state_mtx_);
    if (!ok) {
      // Remove read hold.
      RemoveHold();
      {
        SPDLOG_DEBUG("Change read-state {} --> {}", static_cast<std::uint8_t>(read_state_),
                     static_cast<std::uint8_t>(StreamState::Error));
        read_state_ = StreamState::Error;
        SPDLOG_WARN("Failed to read from telemetry stream from {}", peer_address_);

        // Sync write state
        switch (write_state_) {
          case StreamState::Created: {
            // Not reachable
            break;
          }
          case StreamState::Ready: {
            write_state_ = StreamState::Closed;
            // There is no inflight write request, remove write hold on its behalf.
            RemoveHold();
            state_cv_.SignalAll();
            break;
          }
          case StreamState::Inflight: {
            write_state_ = StreamState::Closing;
            break;
          }
          case StreamState::Closing:
          case StreamState::Error:
          case StreamState::Closed: {
            break;
          }
        }
      }
      return;
    } else if (StreamState::Closing == read_state_) {
      SPDLOG_DEBUG("Change read-state {} --> {}", static_cast<std::uint8_t>(read_state_),
                   static_cast<std::uint8_t>(StreamState::Closed));
      read_state_ = StreamState::Closed;
      state_cv_.SignalAll();
      return;
    }
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

  {
    absl::MutexLock lk(&state_mtx_);
    if (StreamState::Inflight == read_state_) {
      SPDLOG_DEBUG("Spawn new read op, read-state={}", static_cast<std::uint8_t>(read_state_));
      StartRead(&read_);
    } else if (read_state_ == StreamState::Closing) {
      SPDLOG_DEBUG("Change read-state {} --> {}", static_cast<std::uint8_t>(read_state_),
                   static_cast<std::uint8_t>(StreamState::Closed));
      read_state_ = StreamState::Closed;
      state_cv_.SignalAll();
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

void TelemetryBidiReactor::fireRead() {
  absl::MutexLock lk(&state_mtx_);
  if (StreamState::Created != read_state_) {
    SPDLOG_DEBUG("Further read from {} is not allowded due to stream-state={}", peer_address_,
                 static_cast<std::uint8_t>(read_state_));
    return;
  }
  SPDLOG_DEBUG("Change read-state {} --> {}", static_cast<std::uint8_t>(read_state_),
               static_cast<std::uint8_t>(StreamState::Ready));
  read_state_ = StreamState::Ready;
  AddHold();
  SPDLOG_DEBUG("Change read-state {} --> {}", static_cast<std::uint8_t>(read_state_),
               static_cast<std::uint8_t>(StreamState::Inflight));
  read_state_ = StreamState::Inflight;
  StartRead(&read_);
}

void TelemetryBidiReactor::write(TelemetryCommand command) {
  SPDLOG_DEBUG("{}#write", peer_address_);
  {
    absl::MutexLock lk(&state_mtx_);
    // Reject incoming write commands if the stream state is closing or has witnessed some error.
    switch (write_state_) {
      case StreamState::Closing:
      case StreamState::Error:
      case StreamState::Closed:
        return;
      default:
        // no-op
        break;
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
  {
    absl::MutexLock lk(&state_mtx_);
    if (StreamState::Error == write_state_ || StreamState::Closed == write_state_) {
      SPDLOG_WARN("Further write to {} is not allowded due to stream-state={}", peer_address_,
                  static_cast<std::uint8_t>(write_state_));
      return;
    }
  }

  {
    absl::MutexLock lk(&writes_mtx_);
    if (writes_.empty() && StreamState::Closing != write_state_) {
      SPDLOG_DEBUG("No TelemtryCommand to write. Peer={}", peer_address_);
      return;
    }

    if (StreamState::Ready == write_state_) {
      write_state_ = StreamState::Inflight;
    }

    if (writes_.empty()) {
      // Tell server there is no more write requests.
      StartWritesDone();
    } else {
      SPDLOG_DEBUG("Writing telemetry command to {}: {}", peer_address_, writes_.front().DebugString());
      StartWrite(&(writes_.front()));
    }
  }
}

void TelemetryBidiReactor::fireClose() {
  SPDLOG_INFO("{}#fireClose", peer_address_);

  {
    // Acquire state lock
    absl::MutexLock lk(&state_mtx_);

    // Transition read state
    switch (read_state_) {
      case StreamState::Created:
      case StreamState::Ready: {
        SPDLOG_DEBUG("Change read-state {} --> {}", static_cast<std::uint8_t>(read_state_),
                     static_cast<std::uint8_t>(StreamState::Closed));
        read_state_ = StreamState::Closed;
        state_cv_.SignalAll();
        break;
      }

      case StreamState::Inflight: {
        SPDLOG_DEBUG("Change read-state {} --> {}", static_cast<std::uint8_t>(read_state_),
                     static_cast<std::uint8_t>(StreamState::Closing));
        read_state_ = StreamState::Closing;
        break;
      }
      case StreamState::Closing: {
        break;
      }
      case StreamState::Closed:
      case StreamState::Error: {
        state_cv_.SignalAll();
        break;
      }
    }

    // Transition write state
    switch (write_state_) {
      case StreamState::Created:
      case StreamState::Ready:
      case StreamState::Inflight: {
        SPDLOG_DEBUG("Change write-state {} --> {}", static_cast<std::uint8_t>(read_state_),
                     static_cast<std::uint8_t>(StreamState::Closing));
        write_state_ = StreamState::Closing;
        break;
      }
      case StreamState::Closing: {
        break;
      }
      case StreamState::Closed:
      case StreamState::Error: {
        state_cv_.SignalAll();
        break;
      }
    }
  }

  if (StreamState::Closing == write_state_) {
    tryWriteNext();
  }

  {
    // Acquire state lock
    absl::MutexLock lk(&state_mtx_);
    while ((StreamState::Closed != read_state_ && StreamState::Error != read_state_) ||
           (StreamState::Closed != write_state_ && StreamState::Error != write_state_)) {
      if (state_cv_.WaitWithTimeout(&state_mtx_, absl::Seconds(1))) {
        SPDLOG_WARN("StreamState CondVar timed out before getting signalled: read-state={}, write-state={}",
                    static_cast<uint8_t>(read_state_), static_cast<uint8_t>(write_state_));
      }
    }
  }
}

void TelemetryBidiReactor::OnWritesDoneDone(bool ok) {
  SPDLOG_DEBUG("{}#OnWritesDoneDone", peer_address_);
  assert(StreamState::Closing == write_state_);

  absl::MutexLock lk(&state_mtx_);
  // Remove the hold for the write stream.
  RemoveHold();

  if (!ok) {
    write_state_ = StreamState::Error;
    SPDLOG_WARN("Previous telemetry write to {} failed", peer_address_);
  } else {
    write_state_ = StreamState::Closed;
    SPDLOG_DEBUG("{}#OnWritesDoneDone", peer_address_);
  }
  state_cv_.SignalAll();
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
    absl::MutexLock lk(&state_mtx_);
    if (StreamState::Error != read_state_) {
      SPDLOG_DEBUG("Change read-state {} --> {}", static_cast<std::uint8_t>(read_state_),
                   static_cast<std::uint8_t>(StreamState::Closed));
      read_state_ = StreamState::Closed;
    }
    if (StreamState::Error != write_state_) {
      SPDLOG_DEBUG("Change write-state {} --> {}", static_cast<std::uint8_t>(read_state_),
                   static_cast<std::uint8_t>(StreamState::Closed));
      write_state_ = StreamState::Closed;
    }
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
    absl::MutexLock lk(&state_mtx_);
    SPDLOG_DEBUG("Change write-state {} --> {}", static_cast<std::uint8_t>(read_state_),
                 static_cast<std::uint8_t>(StreamState::Error));
    read_state_ = StreamState::Error;
    state_cv_.SignalAll();
    SPDLOG_WARN("Read of initial-metadata failed from {}", peer_address_);
    return;
  }

  SPDLOG_DEBUG("Received initial metadata from {}", peer_address_);
}

ROCKETMQ_NAMESPACE_END
