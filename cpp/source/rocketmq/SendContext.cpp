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
#include "SendContext.h"

#include <system_error>

#include "ProducerImpl.h"
#include "PublishStats.h"
#include "Tag.h"
#include "opencensus/trace/span.h"
#include "rocketmq/ErrorCode.h"
#include "rocketmq/SendReceipt.h"
#include "spdlog/spdlog.h"

ROCKETMQ_NAMESPACE_BEGIN

void SendContext::onSuccess(const SendResult& send_result) noexcept {
  {
    // Mark end of send-message span.
    span_.SetStatus(opencensus::trace::StatusCode::OK);
    span_.End();
  }

  auto producer = producer_.lock();
  if (!producer) {
    SPDLOG_WARN("Producer has been destructed");
    return;
  }

  // Collect metrics
  {
    auto duration = std::chrono::steady_clock::now() - request_time_;
    opencensus::stats::Record({{producer->stats().latency(), MixAll::millisecondsOf(duration)}},
                              {
                                  {Tag::topicTag(), message_->topic()},
                                  {Tag::clientIdTag(), producer->config().client_id},
                                  {Tag::invocationStatusTag(), "success"},
                              });
  }

  // send_receipt.traceContext(opencensus::trace::propagation::ToTraceParentHeader(span_.context()));
  SendReceipt send_receipt = {};
  send_receipt.target = send_result.target;
  send_receipt.message_id = send_result.message_id;
  send_receipt.transaction_id = send_result.transaction_id;
  send_receipt.recall_handle = send_result.recall_handle;
  send_receipt.message = std::move(message_);
  callback_(send_result.ec, send_receipt);
}

void SendContext::onFailure(const std::error_code& ec) noexcept {
  {
    // Mark end of the send-message span.
    span_.SetStatus(opencensus::trace::StatusCode::INTERNAL);
    span_.End();
  }

  auto producer = producer_.lock();
  if (!producer) {
    SPDLOG_WARN("Producer has been destructed");
    return;
  }

  // Collect metrics
  {
    auto duration = std::chrono::steady_clock::now() - request_time_;
    opencensus::stats::Record({{producer->stats().latency(), MixAll::millisecondsOf(duration)}},
                              {
                                  {Tag::topicTag(), message_->topic()},
                                  {Tag::clientIdTag(), producer->config().client_id},
                                  {Tag::invocationStatusTag(), "failure"},
                              });
  }

  if (++attempt_times_ >= producer->maxAttemptTimes()) {
    SPDLOG_WARN("Retried {} times, which exceeds the limit: {}", attempt_times_, producer->maxAttemptTimes());
    SendReceipt receipt{};
    receipt.message = std::move(message_);
    callback_(ec, receipt);
    return;
  }

  if (candidates_.empty()) {
    SPDLOG_WARN("No alternative hosts to perform additional retries");
    SendReceipt receipt{};
    receipt.message = std::move(message_);
    callback_(ec, receipt);
    return;
  }

  auto message_queue = candidates_[attempt_times_ % candidates_.size()];
  request_time_ = std::chrono::steady_clock::now();

  auto ctx = shared_from_this();
  // If publish message requests are throttled, retry after backoff
  if (ErrorCode::TooManyRequests == ec) {
    auto&& backoff = producer->backoff(attempt_times_);
    SPDLOG_DEBUG("Publish message[topic={}, message-id={}] is throttled. Retry after {}ms", message_->topic(),
                 message_->id(), MixAll::millisecondsOf(backoff));
    auto retry_cb = [=]() { producer->sendImpl(ctx); };
    producer->schedule("retry-after-send-throttle", retry_cb, backoff);
    return;
  }
  producer->sendImpl(ctx);
}

ROCKETMQ_NAMESPACE_END