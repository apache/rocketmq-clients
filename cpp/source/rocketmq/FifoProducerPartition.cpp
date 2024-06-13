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
    absl::MutexLock lk(&messages_mtx_);

    if (messages_.empty()) {
      SPDLOG_DEBUG("There is no more messages to send");
      return;
    }

    FifoContext& ctx = messages_.front();
    MessageConstPtr message = std::move(ctx.message);
    SendCallback send_callback = ctx.callback;

    std::shared_ptr<FifoProducerPartition> partition = shared_from_this();
    auto fifo_callback = [=](const std::error_code& ec, const SendReceipt& receipt) mutable {
      partition->onComplete(ec, receipt, send_callback);
    };
    SPDLOG_DEBUG("Sending FIFO message from {}", name_);
    producer_->send(std::move(message), fifo_callback);
    messages_.pop_front();
    SPDLOG_DEBUG("In addition to the inflight one, there is {} messages pending in {}", messages_.size(), name_);
  } else {
    SPDLOG_DEBUG("There is an inflight message");
  }
}

void FifoProducerPartition::onComplete(const std::error_code& ec, const SendReceipt& receipt, SendCallback& callback) {
  if (ec) {
    SPDLOG_INFO("{} completed with a failure: {}", name_, ec.message());
  } else {
    SPDLOG_DEBUG("{} completed OK", name_);
  }

  if (!ec) {
    callback(ec, receipt);
    // update inflight status
    bool expected = true;
    if (inflight_.compare_exchange_strong(expected, false, std::memory_order_relaxed)) {
      trySend();
    } else {
      SPDLOG_ERROR("{}: Unexpected inflight status", name_);
    }
    return;
  }

  // Put the message back to the front of the list
  SendReceipt& receipt_mut = const_cast<SendReceipt&>(receipt);
  FifoContext retry_context(std::move(receipt_mut.message), callback);
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
