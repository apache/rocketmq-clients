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
#include "ConsumeMessageServiceImpl.h"

#include <atomic>

#include "ConsumeStats.h"
#include "ConsumeTask.h"
#include "PushConsumerImpl.h"
#include "ThreadPoolImpl.h"
#include "rocketmq/ErrorCode.h"
#include "rocketmq/Logger.h"
#include "spdlog/spdlog.h"

ROCKETMQ_NAMESPACE_BEGIN

ConsumeMessageServiceImpl::ConsumeMessageServiceImpl(std::weak_ptr<PushConsumerImpl> consumer,
                                                     int thread_count,
                                                     MessageListener message_listener)
    : state_(State::CREATED),
      thread_count_(thread_count),
      pool_(absl::make_unique<ThreadPoolImpl>(thread_count_)),
      consumer_(std::move(consumer)),
      message_listener_(message_listener) {
}

void ConsumeMessageServiceImpl::start() {
  State expected = State::CREATED;
  if (state_.compare_exchange_strong(expected, State::STARTING, std::memory_order_relaxed)) {
    pool_->start();
    state_.store(State::STARTED, std::memory_order_relaxed);
  }
}

void ConsumeMessageServiceImpl::shutdown() {
  State expected = State::STARTED;
  if (state_.compare_exchange_strong(expected, State::STOPPING, std::memory_order_relaxed)) {
    pool_->shutdown();
    state_.store(State::STOPPED, std::memory_order_relaxed);
  }
}

State ConsumeMessageServiceImpl::state() const {
  return state_.load(std::memory_order_relaxed);
}

void ConsumeMessageServiceImpl::dispatch(std::shared_ptr<ProcessQueue> process_queue,
                                         std::vector<MessageConstSharedPtr> messages) {
  auto consumer = consumer_.lock();
  if (!consumer) {
    return;
  }

  if (consumer->config().subscriber.fifo) {
    if (!consumer->config().subscriber.fifo_consume_accelerator) {
      auto consume_task = std::make_shared<ConsumeTask>(
          shared_from_this(), process_queue, std::move(messages));
      pool_->submit([consume_task]() { consume_task->process(); });
    } else {
      std::map<std::string, std::vector<MessageConstSharedPtr>> grouped_messages;
      std::vector<MessageConstSharedPtr> ungrouped_messages;

      for (const auto& message : messages) {
        if (!message->group().empty()) {
          grouped_messages[message->group()].push_back(message);
        } else {
          ungrouped_messages.push_back(message);
        }
      }

      SPDLOG_INFO("FifoConsumeService accelerator enable, message_count={}, group_count={}",
                  messages.size(), grouped_messages.size() + (ungrouped_messages.empty() ? 0 : 1));

      // C++17 could use [group, msg_list]
      for (const auto& pair : grouped_messages) {
        auto consume_task = std::make_shared<ConsumeTask>(
            shared_from_this(), process_queue, pair.second);
        pool_->submit([consume_task]() { consume_task->process(); });
      }

      if (!ungrouped_messages.empty()) {
        auto consume_task = std::make_shared<ConsumeTask>(
            shared_from_this(), process_queue, ungrouped_messages);
        pool_->submit([consume_task]() { consume_task->process(); });
      }
    }
    return;
  }

  for (const auto& message : messages) {
    auto consume_task = std::make_shared<ConsumeTask>(
        shared_from_this(), process_queue, message);
    pool_->submit([consume_task]() { consume_task->process(); });
  }
}

void ConsumeMessageServiceImpl::submit(std::shared_ptr<ConsumeTask> task) {
  auto consumer = consumer_.lock();
  if (!consumer) {
    return;
  }

  pool_->submit([task]() { task->process(); });
}

void ConsumeMessageServiceImpl::ack(const Message& message, std::function<void(const std::error_code&)> cb) {
  auto consumer = consumer_.lock();
  if (!consumer) {
    return;
  }

  std::weak_ptr<PushConsumerImpl> client(consumer_);
  const auto& topic = message.topic();

  const auto& message_id = message.id();
  const auto& receipt_handle = message.extension().receipt_handle;

  auto callback = [cb, client, topic, message_id, receipt_handle](const std::error_code& ec) {
    auto consumer = client.lock();

    // If the receipt_handle was already expired, it is safe to treat it as success.
    if (ec == ErrorCode::InvalidReceiptHandle) {
      SPDLOG_WARN("Broker complained bad receipt handle on ack message[MsgId={}, ReceiptHandle={}]", message_id,
                  receipt_handle);
      cb(ErrorCode::Success);
      return;
    }

    cb(ec);
  };

  consumer->ack(message, callback);
}

void ConsumeMessageServiceImpl::nack(const Message& message, std::function<void(const std::error_code&)> cb) {
  auto consumer = consumer_.lock();
  if (!consumer) {
    return;
  }
  consumer->nack(message, cb);
}

void ConsumeMessageServiceImpl::forward(const Message& message, std::function<void(const std::error_code&)> cb) {
  auto consumer = consumer_.lock();
  if (!consumer) {
    return;
  }
  consumer->forwardToDeadLetterQueue(message, cb);
}

void ConsumeMessageServiceImpl::schedule(std::shared_ptr<ConsumeTask> task, std::chrono::milliseconds delay) {
}

std::size_t ConsumeMessageServiceImpl::maxDeliveryAttempt() {
  std::shared_ptr<PushConsumerImpl> consumer = consumer_.lock();
  if (!consumer) {
    SPDLOG_WARN("The consumer has already destructed");
    return 0;
  }

  return consumer->maxDeliveryAttempts();
}

std::weak_ptr<PushConsumerImpl> ConsumeMessageServiceImpl::consumer() {
  return consumer_;
}

bool ConsumeMessageServiceImpl::preHandle(const Message& message) {
  return true;
}

bool ConsumeMessageServiceImpl::postHandle(const Message& message, ConsumeResult result) {
  return true;
}

ROCKETMQ_NAMESPACE_END