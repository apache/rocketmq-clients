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

#include "rocketmq/SimpleConsumer.h"

#include "SimpleConsumerImpl.h"
#include "StaticNameServerResolver.h"
#include "rocketmq/ErrorCode.h"

ROCKETMQ_NAMESPACE_BEGIN

SimpleConsumerBuilder::SimpleConsumerBuilder() : configuration_(Configuration::newBuilder().build()) {
}

SimpleConsumerBuilder SimpleConsumer::newBuilder() {
  return {};
}

SimpleConsumer::SimpleConsumer(std::string group) : impl_(std::make_shared<SimpleConsumerImpl>(group)) {
}

void SimpleConsumer::start() {
  impl_->start();
}

void SimpleConsumer::subscribe(std::string topic, FilterExpression filter_expression) {
  impl_->subscribe(topic, filter_expression);
}

void SimpleConsumer::unsubscribe(const std::string& topic) {
  impl_->unsubscribe(topic);
}

void SimpleConsumer::receive(std::size_t limit,
                             std::chrono::milliseconds invisible_duration,
                             std::error_code& ec,
                             std::vector<MessageConstSharedPtr>& messages) {
  auto mtx = std::make_shared<absl::Mutex>();
  auto cv = std::make_shared<absl::CondVar>();
  bool completed = false;
  auto callback = [&, mtx, cv](const std::error_code& code, const std::vector<MessageConstSharedPtr>& result) {
    {
      absl::MutexLock lk(mtx.get());
      if (code && code != ErrorCode::NoContent) {
        ec = code;
        SPDLOG_WARN("Failed to receive message. Cause: {}", code.message());
      }
      completed = true;
      messages.insert(messages.end(), result.begin(), result.end());
    }
    cv->SignalAll();
  };

  impl_->receive(limit, invisible_duration, callback);

  {
    absl::MutexLock lk(mtx.get());
    while (!completed) {
      cv->Wait(mtx.get());
    }
  }
}

void SimpleConsumer::asyncReceive(std::size_t limit,
                                  std::chrono::milliseconds invisible_duration,
                                  ReceiveCallback callback) {
  impl_->receive(limit, invisible_duration, callback);
}

void SimpleConsumer::ack(const Message& message, std::error_code& ec) {
  impl_->ack(message, ec);
}

void SimpleConsumer::asyncAck(const Message& message, AckCallback callback) {
  impl_->ackAsync(message, callback);
}

void SimpleConsumer::changeInvisibleDuration(const Message& message, std::string& receipt_handle,
                                             std::chrono::milliseconds duration,
                                             std::error_code& ec) {
  auto mtx = std::make_shared<absl::Mutex>();
  auto cv = std::make_shared<absl::CondVar>();
  bool completed = false;

  auto callback =
      [&, mtx, cv](const std::error_code& code, std::string& server_receipt_handle) {
    {
      absl::MutexLock lk(mtx.get());
      completed = true;
      ec = code;
      if (!ec) {
        receipt_handle = server_receipt_handle;
      }
    }
    cv->Signal();
  };

  impl_->changeInvisibleDuration(message, receipt_handle, duration, callback);

  {
    absl::MutexLock lk(mtx.get());
    if (!completed) {
      cv->Wait(mtx.get());
    }
  }
}

void SimpleConsumer::asyncChangeInvisibleDuration(const Message& message, std::string& receipt_handle,
                                                  std::chrono::milliseconds duration,
                                                  ChangeInvisibleDurationCallback callback) {
  impl_->changeInvisibleDuration(message, receipt_handle, duration, callback);
}

SimpleConsumer SimpleConsumerBuilder::build() {
  SimpleConsumer simple_consumer(group_);

  simple_consumer.impl_->withRequestTimeout(configuration_.requestTimeout());
  simple_consumer.impl_->withNameServerResolver(std::make_shared<StaticNameServerResolver>(configuration_.endpoints()));
  simple_consumer.impl_->withResourceNamespace(configuration_.resourceNamespace());
  simple_consumer.impl_->withCredentialsProvider(configuration_.credentialsProvider());
  simple_consumer.impl_->withReceiveMessageTimeout(await_duration_);
  simple_consumer.impl_->withSsl(configuration_.withSsl());

  for (const auto& entry : subscriptions_) {
    simple_consumer.impl_->subscribe(entry.first, entry.second);
  }

  simple_consumer.start();
  return simple_consumer;
}

ROCKETMQ_NAMESPACE_END