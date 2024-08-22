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

#include <memory>
#include <system_error>

#include "Protocol.h"
#include "SendResult.h"
#include "TransactionImpl.h"
#include "opencensus/trace/span.h"
#include "rocketmq/Message.h"
#include "rocketmq/SendCallback.h"

ROCKETMQ_NAMESPACE_BEGIN

class ProducerImpl;

class SendContext : public std::enable_shared_from_this<SendContext> {
public:
  SendContext(std::weak_ptr<ProducerImpl> producer,
              MessageConstPtr message,
              SendCallback callback,
              std::vector<rmq::MessageQueue> candidates)
      : producer_(std::move(producer)),
        message_(std::move(message)),
        callback_(std::move(callback)),
        candidates_(std::move(candidates)),
        span_(opencensus::trace::Span::BlankSpan()) {
  }

  void onSuccess(const SendResult& send_result) noexcept;

  void onFailure(const std::error_code& ec) noexcept;

  const rmq::MessageQueue& messageQueue() const {
    int index = attempt_times_ % candidates_.size();
    return candidates_[index];
  }

  std::weak_ptr<ProducerImpl> producer_;
  MessageConstPtr message_;
  std::size_t attempt_times_{0};
  SendCallback callback_;

  /**
   * @brief Once the first publish attempt failed, the following routable
   * message queues are employed.
   *
   */
  std::vector<rmq::MessageQueue> candidates_;

  /**
   * @brief The on-going span. Should be terminated in the callback functions.
   */
  opencensus::trace::Span span_;

  std::chrono::steady_clock::time_point request_time_{std::chrono::steady_clock::now()};
};

ROCKETMQ_NAMESPACE_END