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
#include <mutex>
#include <string>
#include <system_error>

#include "ConsumeMessageService.h"
#include "ThreadPool.h"
#include "absl/container/flat_hash_map.h"
#include "rocketmq/State.h"

ROCKETMQ_NAMESPACE_BEGIN

class PushConsumerImpl;

class ConsumeMessageServiceImpl
    : public ConsumeMessageService,
      public std::enable_shared_from_this<ConsumeMessageServiceImpl> {
public:
  ConsumeMessageServiceImpl(std::weak_ptr<PushConsumerImpl> consumer,
                            int thread_count, MessageListener message_listener);

  ~ConsumeMessageServiceImpl() override = default;

  /**
   * Make it noncopyable.
   */
  ConsumeMessageServiceImpl(const ConsumeMessageServiceImpl &other) = delete;

  ConsumeMessageServiceImpl &operator=(const ConsumeMessageServiceImpl &other) = delete;

  void start() override;

  void shutdown() override;

  MessageListener &listener() override { return message_listener_; }

  bool preHandle(const Message &message) override;

  bool postHandle(const Message &message, ConsumeResult result) override;

  void submit(std::shared_ptr<ConsumeTask> task) override;

  void dispatch(std::shared_ptr<ProcessQueue> process_queue,
                std::vector<MessageConstSharedPtr> messages) override;

  void ack(const Message &message,
           std::function<void(const std::error_code &)> cb) override;

  void nack(const Message &message,
            std::function<void(const std::error_code &)> cb) override;

  void forward(const Message &message,
               std::function<void(const std::error_code &)> cb) override;

  void schedule(std::shared_ptr<ConsumeTask> task,
                std::chrono::milliseconds delay) override;

  std::size_t maxDeliveryAttempt() override;

  std::weak_ptr<PushConsumerImpl> consumer() override;

  /**
   * Current state of the consume message service.
   */
  State state() const;

protected:
  std::atomic<State> state_;

  int thread_count_;
  std::unique_ptr<ThreadPool> pool_;
  std::weak_ptr<PushConsumerImpl> consumer_;

  MessageListener message_listener_;
};

ROCKETMQ_NAMESPACE_END