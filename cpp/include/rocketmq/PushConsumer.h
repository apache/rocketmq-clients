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
#include <string>

#include "Configuration.h"
#include "CredentialsProvider.h"
#include "Executor.h"
#include "ExpressionType.h"
#include "FilterExpression.h"
#include "Logger.h"
#include "MessageListener.h"

ROCKETMQ_NAMESPACE_BEGIN

class PushConsumerImpl;
class PushConsumerBuilder;

class PushConsumer {
public:
  static PushConsumerBuilder newBuilder();

  void subscribe(std::string topic, FilterExpression filter_expression);

  void unsubscribe(const std::string& topic);

private:
  friend class PushConsumerBuilder;

  explicit PushConsumer(std::shared_ptr<PushConsumerImpl> impl)
      : impl_(std::move(impl)) {
  } 

  std::shared_ptr<PushConsumerImpl> impl_;
};

class PushConsumerBuilder {
public:
  PushConsumerBuilder() : configuration_(Configuration::newBuilder().build()) {}
  
  PushConsumerBuilder& withConfiguration(Configuration configuration) {
    configuration_ = std::move(configuration);
    return *this;
  }

  PushConsumerBuilder& withGroup(std::string group) {
    group_ = std::move(group);
    return *this;
  }

  PushConsumerBuilder& withConsumeThreads(std::size_t consume_thread) {
    consume_thread_ = consume_thread;
    return *this;
  }

  PushConsumerBuilder& withListener(MessageListener listener) {
    listener_ = std::move(listener);
    return *this;
  }

  PushConsumerBuilder& subscribe(std::string topic,
                                 FilterExpression filter_expression) {
    subscriptions_.insert({topic, filter_expression});
    return *this;
  }

  PushConsumerBuilder& fifoConsumeAccelerator(bool fifo_consume_accelerator) {
    fifo_consume_accelerator_ = fifo_consume_accelerator;
    return *this;
  }

  PushConsumer build();

private:
  std::string group_;
  Configuration configuration_;
  std::size_t consume_thread_ = 20;
  std::unordered_map<std::string, FilterExpression> subscriptions_;
  MessageListener listener_;
  bool fifo_consume_accelerator_ = false;
};

ROCKETMQ_NAMESPACE_END