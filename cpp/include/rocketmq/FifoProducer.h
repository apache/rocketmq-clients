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

#include <cstddef>
#include <memory>
#include <vector>

#include "Configuration.h"
#include "Message.h"
#include "RocketMQ.h"
#include "SendCallback.h"

ROCKETMQ_NAMESPACE_BEGIN

class FifoProducerImpl;
class FifoProducerBuilder;
class ProducerImpl;

class FifoProducer {
public:
  static FifoProducerBuilder newBuilder();

  void send(MessageConstPtr message, SendCallback callback);

private:
  std::shared_ptr<FifoProducerImpl> impl_;

  explicit FifoProducer(std::shared_ptr<FifoProducerImpl> impl) : impl_(std::move(impl)) {
  }

  void start();

  friend class FifoProducerBuilder;
};

class FifoProducerBuilder {
public:
  FifoProducerBuilder();

  FifoProducerBuilder& withConfiguration(Configuration configuration);

  FifoProducerBuilder& withTopics(const std::vector<std::string>& topics);

  FifoProducerBuilder& withConcurrency(std::size_t concurrency);

  FifoProducer build();

private:
  std::shared_ptr<FifoProducerImpl> impl_;
  std::shared_ptr<ProducerImpl> producer_impl_;
};

ROCKETMQ_NAMESPACE_END