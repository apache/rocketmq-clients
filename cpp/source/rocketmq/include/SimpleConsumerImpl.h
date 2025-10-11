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

#include <chrono>

#include "ClientImpl.h"
#include "rocketmq/FilterExpression.h"
#include "rocketmq/SimpleConsumer.h"

using namespace std::chrono;
ROCKETMQ_NAMESPACE_BEGIN

class SimpleConsumerImpl : virtual public ClientImpl, public std::enable_shared_from_this<SimpleConsumerImpl> {
public:
  SimpleConsumerImpl(std::string group);

  ~SimpleConsumerImpl() override;

  void prepareHeartbeatData(rmq::HeartbeatRequest& request) override;

  void buildClientSettings(rmq::Settings& settings) override;

  std::shared_ptr<ClientImpl> self() override {
    return shared_from_this();
  }

  void start() override;

  void shutdown() override;

  void subscribe(std::string topic, FilterExpression expression) LOCKS_EXCLUDED(subscriptions_mtx_);

  void unsubscribe(const std::string& topic) LOCKS_EXCLUDED(subscriptions_mtx_);

  void receive(std::size_t limit, std::chrono::milliseconds invisible_duration, ReceiveCallback callback);

  void ack(const Message& message, std::error_code& ec);

  void ackAsync(const Message& message, AckCallback callback);

  void changeInvisibleDuration(const Message& message, std::string& receipt_handle,
                               std::chrono::milliseconds duration,
                               const ChangeInvisibleDurationCallback callback);

  void withReceiveMessageTimeout(std::chrono::milliseconds receive_timeout) {
    long_polling_duration_ = receive_timeout;
  }

protected:
  void topicsOfInterest(std::vector<std::string> &topics) override;

private:
  absl::flat_hash_map<std::string, FilterExpression> subscriptions_ GUARDED_BY(subscriptions_mtx_);
  absl::Mutex subscriptions_mtx_;

  absl::flat_hash_map<std::string, std::vector<rmq::Assignment>> topic_assignments_ GUARDED_BY(topic_assignments_mtx_);
  absl::Mutex topic_assignments_mtx_;

  std::vector<rmq::Assignment> assignments_;
  absl::Mutex assignments_mtx_ ACQUIRED_AFTER(topic_assignments_mtx_);

  std::size_t refresh_assignment_task_{0};

  static thread_local std::size_t assignment_index_;

  std::chrono::milliseconds long_polling_duration_{MixAll::DefaultReceiveMessageTimeout};

  void refreshAssignments0() LOCKS_EXCLUDED(topic_assignments_mtx_, subscriptions_mtx_);

  void refreshAssignments() LOCKS_EXCLUDED(subscriptions_mtx_);

  void refreshAssignment(const std::string& topic, std::function<void(const std::error_code&)> cb);

  void updateAssignments(const std::string& topic, const std::vector<rmq::Assignment>& assignments)
      LOCKS_EXCLUDED(assignments_mtx_, topic_assignments_mtx_);

  void wrapAckRequest(const Message& message, AckMessageRequest& request);

  void removeAssignmentsByTopic(const std::string& topic) LOCKS_EXCLUDED(topic_assignments_mtx_, assignments_mtx_);
};

ROCKETMQ_NAMESPACE_END