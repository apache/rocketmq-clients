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
#include <chrono>
#include <memory>

#include <spdlog/spdlog.h>

#include "PushConsumerImpl.h"
#include "StaticNameServerResolver.h"
#include "rocketmq/PushConsumer.h"

ROCKETMQ_NAMESPACE_BEGIN

void PushConsumer::subscribe(std::string topic, FilterExpression filter_expression) noexcept {
  try {
    impl_->subscribe(std::move(topic), filter_expression.content_, filter_expression.type_);
  } catch (const std::exception& e) {
    SPDLOG_ERROR("Exception in subscribe: {}", e.what());
  }
}

void PushConsumer::unsubscribe(const std::string& topic) noexcept {
  try {
    impl_->unsubscribe(topic);
  } catch (const std::exception& e) {
    SPDLOG_ERROR("Exception in unsubscribe: {}", e.what());
  }
}

PushConsumerBuilder PushConsumer::newBuilder() {
  return {};
}

PushConsumer PushConsumerBuilder::build() {
  auto impl = std::make_shared<PushConsumerImpl>(group_);
  impl->registerMessageListener(listener_);
  for (const auto& entry : subscriptions_) {
    impl->subscribe(entry.first, entry.second.content_, entry.second.type_);
  }
  impl->consumeThreadPoolSize(consume_thread_);
  impl->withNameServerResolver(std::make_shared<StaticNameServerResolver>(configuration_.endpoints()));
  impl->withResourceNamespace(configuration_.resourceNamespace());
  impl->withCredentialsProvider(configuration_.credentialsProvider());
  impl->withRequestTimeout(configuration_.requestTimeout());
  impl->withFifoConsumeAccelerator(fifo_consume_accelerator_);
  impl->withCallbackThreads(configuration_.callbackThreads());
  impl->withSsl(configuration_.withSsl());
  impl->start();
  return PushConsumer(impl);
}

ROCKETMQ_NAMESPACE_END