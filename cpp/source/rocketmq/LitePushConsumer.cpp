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
#include "rocketmq/LitePushConsumer.h"

#include <memory>
#include <stdexcept>
#include <string>

#include "LitePushConsumerImpl.h"
#include "StaticNameServerResolver.h"
#include "rocketmq/Configuration.h"
#include "rocketmq/OffsetOption.h"

ROCKETMQ_NAMESPACE_BEGIN

LitePushConsumerBuilder LitePushConsumer::newBuilder() {
  return {};
}

void LitePushConsumer::subscribeLite(const std::string& lite_topic, std::error_code& ec) {
  impl_->subscribeLite(lite_topic, nullptr, ec);
}

void LitePushConsumer::subscribeLite(const std::string& lite_topic,
                                     const OffsetOption& offset_option,
                                     std::error_code& ec) {
  impl_->subscribeLite(lite_topic, &offset_option, ec);
}

void LitePushConsumer::unsubscribeLite(const std::string& lite_topic, std::error_code& ec) {
  impl_->unsubscribeLite(lite_topic, ec);
}

std::set<std::string> LitePushConsumer::getLiteTopicSet() const {
  return impl_->getLiteTopicSet();
}

std::string LitePushConsumer::getConsumerGroup() const {
  return impl_->groupName();
}

void LitePushConsumer::shutdown() {
  impl_->shutdown();
}

LitePushConsumer LitePushConsumerBuilder::build() {
  if (bind_topic_.empty()) {
    throw std::invalid_argument("bindTopic has not been set yet");
  }
  if (group_.empty()) {
    throw std::invalid_argument("group has not been set yet");
  }
  if (!listener_) {
    throw std::invalid_argument("messageListener has not been set yet");
  }

  auto impl = std::make_shared<LitePushConsumerImpl>(group_, bind_topic_);

  // Register message listener
  impl->registerMessageListener(listener_);

  // Configure consumer parameters
  impl->consumeThreadPoolSize(consume_threads_);

  // Set name server resolver
  impl->withNameServerResolver(std::make_shared<StaticNameServerResolver>(configuration_.endpoints()));

  // Set resource namespace
  impl->withResourceNamespace(configuration_.resourceNamespace());

  // Set credentials provider
  impl->withCredentialsProvider(configuration_.credentialsProvider());

  // Set request timeout
  impl->withRequestTimeout(configuration_.requestTimeout());

  // Set FIFO consume accelerator
  impl->withFifoConsumeAccelerator(fifo_consume_accelerator_);

  // Set callback threads
  impl->withCallbackThreads(configuration_.callbackThreads());

  // Set SSL
  impl->withSsl(configuration_.withSsl());

  // Start the consumer
  impl->start();

  return LitePushConsumer(impl);
}

ROCKETMQ_NAMESPACE_END
