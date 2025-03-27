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

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <set>

#include "ClientManager.h"
#include "MessageExt.h"
#include "MixAll.h"
#include "ProcessQueue.h"
#include "ReceiveMessageCallback.h"
#include "TopicAssignmentInfo.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "rocketmq/FilterExpression.h"

ROCKETMQ_NAMESPACE_BEGIN

class PushConsumerImpl;

/**
 * @brief Once messages are fetched(either pulled or popped) from remote server, they are firstly put into cache.
 * Dispatcher thread, after waking up, will submit them into thread-pool. Messages at this phase are called "inflight"
 * state. Once messages are processed by user-passed-in callback, their quota will be released for future incoming
 * messages.
 */
class ProcessQueueImpl : virtual public ProcessQueue {
public:
  ProcessQueueImpl(rmq::MessageQueue message_queue,
                   FilterExpression filter_expression,
                   std::weak_ptr<PushConsumerImpl> consumer,
                   std::shared_ptr<ClientManager> client_instance);

  ~ProcessQueueImpl() override;

  void callback(std::shared_ptr<AsyncReceiveMessageCallback> callback) override;

  bool expired() const override;

  bool shouldThrottle() const override;

  const FilterExpression& getFilterExpression() const override;

  std::weak_ptr<PushConsumerImpl> getConsumer() override;

  std::shared_ptr<ClientManager> getClientManager() override;

  void receiveMessage(std::string& attempt_id) override;

  const std::string& simpleName() const override {
    return simple_name_;
  }

  std::string topic() const override {
    return message_queue_.topic().name();
  }

   std::uint64_t cachedMessageQuantity() const override;

   std::uint64_t cachedMessageMemory() const override;

  /**
   * Put message fetched from broker into cache.
   *
   * @param messages
   */
  void accountCache(const std::vector<MessageConstSharedPtr>& messages) override;

  void syncIdleState() override {
    idle_since_ = std::chrono::steady_clock::now();
  }

  void release(uint64_t body_size) override;

  const rmq::MessageQueue& messageQueue() const override {
    return message_queue_;
  }

private:
  rmq::MessageQueue message_queue_;

  /**
   * Expression used to filter message in the server side.
   */
  const FilterExpression filter_expression_;

  std::chrono::milliseconds invisible_time_;

  std::chrono::steady_clock::time_point idle_since_{std::chrono::steady_clock::now()};

  absl::Time create_timestamp_{absl::Now()};

  std::string simple_name_;

  std::weak_ptr<PushConsumerImpl> consumer_;
  std::shared_ptr<ClientManager> client_manager_;

  std::shared_ptr<AsyncReceiveMessageCallback> receive_callback_;

  /**
   * @brief Quantity of the cached messages.
   *
   */
  std::atomic<uint32_t> cached_message_quantity_;

  /**
   * @brief Total body memory size of the cached messages.
   *
   */
  std::atomic<uint64_t> cached_message_memory_;

  void popMessage(std::string& attempt_id);

  void wrapPopMessageRequest(absl::flat_hash_map<std::string, std::string>& metadata,
                             rmq::ReceiveMessageRequest& request, std::string& attempt_id);

  void wrapFilterExpression(rmq::FilterExpression* filter_expression);
};

ROCKETMQ_NAMESPACE_END