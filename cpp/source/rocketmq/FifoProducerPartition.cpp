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
#include "FifoProducerPartition.h"

#include "absl/synchronization/mutex.h"

#include <atomic>
#include <memory>
#include <system_error>

#include "FifoContext.h"
#include "rocketmq/Message.h"
#include "rocketmq/RocketMQ.h"
#include "rocketmq/SendCallback.h"
#include "rocketmq/SendReceipt.h"
#include "spdlog/spdlog.h"

ROCKETMQ_NAMESPACE_BEGIN

void FifoProducerPartition::add(FifoContext&& context) {
  {
    absl::MutexLock lk(&messages_mtx_);
    messages_.emplace_back(std::move(context));
    SPDLOG_DEBUG("{} has {} pending messages after #add", name_, messages_.size());
  }

  trySend();
}

void FifoProducerPartition::trySend() {
  bool expected = false;
  if (inflight_.compare_exchange_strong(expected, true, std::memory_order_relaxed)) {
    MessageConstPtr message;
    SendCallback send_callback;

    {
      absl::MutexLock lk(&messages_mtx_);
      if (messages_.empty()) {
        SPDLOG_DEBUG("There is no more messages to send");
        inflight_.store(false, std::memory_order_release);
        return;
      }

      FifoContext& ctx = messages_.front();
      message = std::move(ctx.message);
      send_callback = ctx.callback;
      messages_.pop_front();
    }
    // Lock released — producer_->send() and its callbacks run without holding messages_mtx_.
    // This prevents deadlock when send() fails synchronously and invokes the callback
    // on the same thread (onComplete would try to re-acquire messages_mtx_).

    std::shared_ptr<FifoProducerPartition> partition = shared_from_this();
    auto fifo_callback = [=](const std::error_code& ec, SendReceipt&& receipt) mutable {
      partition->onComplete(ec, std::move(receipt), send_callback);
    };
    SPDLOG_DEBUG("Sending FIFO message from {}", name_);
    try {
      producer_->send(std::move(message), fifo_callback);
    } catch (const std::exception& e) {
      SPDLOG_ERROR("Exception in FifoProducerPartition::trySend: {}", e.what());
      // Message is lost (consumed by the throwing send call via unique_ptr move).
      // Invoke user callback directly to avoid null deref from retrying with empty SendReceipt.
      std::error_code ec = std::make_error_code(std::errc::operation_canceled);
      SendReceipt empty;
      send_callback(ec, std::move(empty));
      inflight_.store(false, std::memory_order_release);
    }
  } else {
    SPDLOG_DEBUG("There is an inflight message");
  }
}

void FifoProducerPartition::onComplete(const std::error_code& ec, SendReceipt&& receipt, SendCallback& callback) {
  if (ec) {
    SPDLOG_INFO("{} completed with a failure: {}", name_, ec.message());
  } else {
    SPDLOG_DEBUG("{} completed OK", name_);
  }

  if (!ec) {
    callback(ec, std::move(receipt));
    // update inflight status
    bool expected = true;
    if (inflight_.compare_exchange_strong(expected, false, std::memory_order_relaxed)) {
      trySend();
    } else {
      SPDLOG_ERROR("{}: Unexpected inflight status", name_);
    }
    return;
  }

  // Put the message back to the front of the list.
  FifoContext retry_context(std::move(receipt.message), callback);
  {
    absl::MutexLock lk(&messages_mtx_);
    messages_.emplace_front(std::move(retry_context));
  }

  // Update inflight status
  bool expected = true;
  if (inflight_.compare_exchange_strong(expected, false, std::memory_order_relaxed)) {
    trySend();
  } else {
    SPDLOG_ERROR("Unexpected inflight status");
  }
}

ROCKETMQ_NAMESPACE_END
