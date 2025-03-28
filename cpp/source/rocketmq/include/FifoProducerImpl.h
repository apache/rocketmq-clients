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

#include "FifoProducerPartition.h"
#include "ProducerImpl.h"
#include "fmt/format.h"
#include "rocketmq/Message.h"
#include "rocketmq/SendCallback.h"

ROCKETMQ_NAMESPACE_BEGIN

class FifoProducerImpl : std::enable_shared_from_this<FifoProducerImpl> {
public:
  FifoProducerImpl(std::shared_ptr<ProducerImpl> producer, std::size_t concurrency)
      : producer_(producer), concurrency_(concurrency), partitions_(concurrency) {
    for (auto i = 0; i < concurrency; i++) {
      partitions_[i] = std::make_shared<FifoProducerPartition>(producer_, fmt::format("slot-{}", i));
    }
  };

  void send(MessageConstPtr message, SendCallback callback);

  std::shared_ptr<ProducerImpl>& internalProducer() {
    return producer_;
  }

private:
  std::shared_ptr<ProducerImpl> producer_;
  std::vector<std::shared_ptr<FifoProducerPartition>> partitions_;
  std::size_t concurrency_;
  std::hash<std::string> hash_fn_;
};

ROCKETMQ_NAMESPACE_END